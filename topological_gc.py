"""
Discrete-event simulator for Topological Garbage Collection of intermediate
datasets in DAG-oriented workflow management systems.

Reference implementation accompanying the article:
    "Детерминированный метод управления жизненным циклом промежуточных
     данных в DAG-ориентированных системах многокаскадной обработки"

The simulator models a workflow as a directed acyclic graph G = (D, J, E)
of datasets D and tasks J, with one input and (in the singleton-consumer
convention used in the article) one independent output per downstream
consumer. The core primitives mirror the article one-to-one:

    Consumers(d) = { j ∈ J : (d, j) ∈ E }

    CanDelete(d) ⇔  ¬IsBoundary(d)
                    ∧ ∀ j ∈ Consumers(d): status(j) ∈ T,
        where T = {FINISHED, FAILED, CANCELLED}.

    Active(G) = { j ∈ J : status(j) ∉ T }.

Two cleanup policies are supported:

  - "immediate": after every task transitions to a terminal status, all
    datasets satisfying CanDelete are freed at once.
  - "deferred": no intermediate dataset is freed until Active(G) = ∅;
    then a single batch sweep frees all CanDelete datasets.

Boundary datasets — trigger inputs and final outputs — are exempt from
auto-deletion (IsBoundary = True).

This module is intentionally small: a few hundred lines of pure-Python
with no external dependencies. It exists for reproducibility of the
buffer-occupancy plots in the article, not as a production scheduler.
"""

from __future__ import annotations

import heapq
import math
import random
from bisect import bisect_right
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable


class TaskStatus(Enum):
    """Lifecycle states of a WfMS task."""

    DEFINED = "DEFINED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# The set T from the article: terminal (absorbing) statuses.
TERMINAL: frozenset[TaskStatus] = frozenset(
    {TaskStatus.FINISHED, TaskStatus.FAILED, TaskStatus.CANCELLED}
)


@dataclass
class Dataset:
    """An intermediate or boundary dataset in the workflow graph."""

    id: int
    size: float                       # bytes / arbitrary units
    is_boundary: bool = False         # trigger input or final output
    created_at: float | None = None   # materialisation time
    deleted_at: float | None = None   # GC release time


@dataclass
class Task:
    """
    A workflow task with at most one input (WfMS convention).

    ``input_id`` is ``None`` for **source tasks**: their input is a trigger
    dataset that lives on an external input-storage tier and does not
    occupy the tracked buffer. All non-source tasks have exactly one
    input dataset materialised in the buffer.
    """

    id: int
    input_id: int | None
    output_ids: list[int]             # one independent output per consumer
    duration: float
    status: TaskStatus = TaskStatus.DEFINED
    finished_at: float | None = None


class Workflow:
    """
    Workflow = tasks + datasets + cleanup policy.

    The Workflow object owns the back-references from each dataset to its
    consumer tasks: they are populated automatically by ``add_task`` and
    queried by ``consumers(d)``.
    """

    POLICIES = ("immediate", "deferred")

    def __init__(self, policy: str = "immediate") -> None:
        if policy not in self.POLICIES:
            raise ValueError(
                f"policy must be one of {self.POLICIES}, got {policy!r}"
            )
        self.policy = policy
        self.tasks: dict[int, Task] = {}
        self.datasets: dict[int, Dataset] = {}
        self._consumers: dict[int, list[int]] = {}

    # ---- builders --------------------------------------------------------

    def add_dataset(
        self, ds_id: int, size: float, is_boundary: bool = False
    ) -> Dataset:
        if ds_id in self.datasets:
            raise ValueError(f"dataset {ds_id} already exists")
        ds = Dataset(id=ds_id, size=size, is_boundary=is_boundary)
        self.datasets[ds_id] = ds
        self._consumers.setdefault(ds_id, [])
        return ds

    def add_task(
        self,
        task_id: int,
        input_id: int | None,
        output_ids: Iterable[int],
        duration: float,
    ) -> Task:
        if task_id in self.tasks:
            raise ValueError(f"task {task_id} already exists")
        if input_id is not None and input_id not in self.datasets:
            raise ValueError(f"unknown input dataset {input_id}")
        outs = list(output_ids)
        for out in outs:
            if out not in self.datasets:
                raise ValueError(f"unknown output dataset {out}")
        task = Task(
            id=task_id, input_id=input_id, output_ids=outs, duration=duration
        )
        self.tasks[task_id] = task
        if input_id is not None:
            self._consumers.setdefault(input_id, []).append(task_id)
        return task

    # ---- article primitives ---------------------------------------------

    def consumers(self, ds_id: int) -> list[int]:
        """Consumers(d) — task ids that read dataset d as input."""
        return list(self._consumers.get(ds_id, []))

    def can_delete(self, ds_id: int) -> bool:
        """Predicate (1) from the article."""
        ds = self.datasets[ds_id]
        if ds.is_boundary or ds.deleted_at is not None or ds.created_at is None:
            return False
        return all(
            self.tasks[c].status in TERMINAL for c in self.consumers(ds_id)
        )

    def is_active(self) -> bool:
        """Active(G) ≠ ∅ — at least one task is not yet terminal."""
        return any(t.status not in TERMINAL for t in self.tasks.values())

    def occupancy(self) -> float:
        """Total size of datasets currently materialised in the buffer."""
        return sum(
            ds.size
            for ds in self.datasets.values()
            if ds.created_at is not None and ds.deleted_at is None
        )

    # ---- simulation engine ----------------------------------------------

    def simulate(
        self,
        overlap: float = 0.05,
        final_offload_delay: float | None = None,
    ) -> tuple[list[float], list[float]]:
        """
        Deterministic discrete-event simulation.

        Tasks execute one after another in topological order (by id —
        the workflow builders below assign ids consistent with the DAG).
        Each task's output is materialised ``overlap`` time units BEFORE
        the task transitions to FINISHED — this models the realistic case
        in which a producer writes its result and only then signals
        completion. Under the Immediate policy this creates a brief
        window in which both the input and the new output co-exist in the
        buffer, visible as a short upward spike in the trace.

        ``final_offload_delay``, when set, models an external publisher
        (long-term storage / DMS) that picks up each final output
        ``δ`` seconds after it was created and removes it from the
        tracked buffer. With offload enabled the system has a steady
        state; without it (default), final outputs accumulate without
        bound. The choice is the caller's: leave it ``None`` to study
        pure cleanup-policy dynamics, or set it to a realistic pickup
        delay to compare against ``V_max`` as a capacity ceiling.

        Returns
        -------
        (times, occupancies) — two equal-length lists suitable for a step
        plot. Each pair (t_k, o_k) is one event sample.
        """
        if overlap <= 0:
            raise ValueError("overlap must be positive")
        if final_offload_delay is not None and final_offload_delay < 0:
            raise ValueError("final_offload_delay must be ≥ 0")

        times: list[float] = [0.0]
        occ: list[float] = [0.0]
        t = 0.0

        def sample() -> None:
            times.append(t)
            occ.append(self.occupancy())

        for task in sorted(self.tasks.values(), key=lambda x: x.id):
            if task.input_id is not None:
                in_ds = self.datasets[task.input_id]
                if in_ds.created_at is None:
                    in_ds.created_at = t
                    sample()

            task.status = TaskStatus.RUNNING
            t += max(task.duration - overlap, 0.0)

            for out_id in task.output_ids:
                out_ds = self.datasets[out_id]
                if out_ds.created_at is None:
                    out_ds.created_at = t
            sample()

            t += overlap
            task.status = TaskStatus.FINISHED
            task.finished_at = t

            if self.policy == "immediate" and task.input_id is not None:
                # Event-local sweep: finishing this task can only newly satisfy
                # CanDelete for its OWN input dataset (whose consumer set just
                # went terminal) — no need to rescan the whole graph.
                if self.can_delete(task.input_id):
                    self.datasets[task.input_id].deleted_at = t
            sample()

        if self.policy == "deferred" and not self.is_active():
            t += overlap
            for ds_id in list(self.datasets):
                if self.can_delete(ds_id):
                    self.datasets[ds_id].deleted_at = t
            sample()

        # External offload of final outputs (long-term storage pickup).
        if final_offload_delay is not None:
            t += final_offload_delay
            for ds in self.datasets.values():
                if (
                    ds.is_boundary
                    and ds.created_at is not None
                    and ds.deleted_at is None
                ):
                    ds.deleted_at = t
            sample()

        # tail for visual stability
        times.append(t + 1.0)
        occ.append(occ[-1])
        return times, occ


# ---- DAG builders --------------------------------------------------------


def build_chain(
    n: int = 6,
    size: float = 1.0,
    duration: float = 1.0,
    policy: str = "immediate",
) -> Workflow:
    """
    Linear chain:  (trigger) → j1 → d1 → j2 → d2 → … → jn → dn

    j1 is a source task: its input is the external trigger dataset, which
    lives on a separate input-storage tier and does not occupy the tracked
    buffer (``input_id=None``). Intermediate datasets d1, …, d_{n-1} live
    in the buffer until released by GC; dn is the final output and is
    boundary-exempt.
    """
    if n < 1:
        raise ValueError("chain length n must be ≥ 1")
    wf = Workflow(policy=policy)
    for i in range(1, n + 1):
        wf.add_dataset(i, size=size, is_boundary=(i == n))
        wf.add_task(
            i,
            input_id=(None if i == 1 else i - 1),
            output_ids=[i],
            duration=duration,
        )
    return wf


def build_fanout_tree(
    depth: int,
    branching: int,
    size: float = 1.0,
    duration: float = 1.0,
    policy: str = "immediate",
) -> Workflow:
    """
    Balanced fan-out tree of the given ``depth`` and ``branching`` factor.

    The root task consumes the external trigger and produces one dataset;
    that dataset is consumed by ``branching`` child tasks, each of which
    produces its own dataset consumed by ``branching`` grandchildren, and
    so on. Tasks/datasets are numbered in breadth-first (topological) order,
    so producers always precede consumers. Leaf datasets are final
    (boundary). ``branching = 1`` degenerates to a linear chain.
    """
    if depth < 1 or branching < 1:
        raise ValueError("depth and branching must be ≥ 1")

    wf = Workflow(policy=policy)
    counter = [0]

    def new_id() -> int:
        counter[0] += 1
        return counter[0]

    root_ds = new_id()
    root_task = new_id()
    wf.add_dataset(root_ds, size=size, is_boundary=(depth == 1))
    wf.add_task(root_task, input_id=None, output_ids=[root_ds],
                duration=duration)

    frontier = [root_ds]
    for level in range(1, depth):
        is_leaf = level == depth - 1
        next_frontier: list[int] = []
        for parent_ds in frontier:
            for _ in range(branching):
                ds = new_id()
                task = new_id()
                wf.add_dataset(ds, size=size, is_boundary=is_leaf)
                wf.add_task(task, input_id=parent_ds, output_ids=[ds],
                            duration=duration)
                next_frontier.append(ds)
        frontier = next_frontier
    return wf


def measure_footprints(wf: Workflow) -> tuple[int, int]:
    """
    Empirically measure the two per-graph footprints of the article model:

      - w (cut-width)  — peak number of simultaneously-live datasets under
        the Immediate policy, i.e. ``phi_imm / d``;
      - M              — total number of datasets the graph passes through
        the buffer, i.e. ``phi_def / d`` (the Deferred footprint).

    The graph is replayed in topological (id) order. Final (boundary)
    datasets are offloaded by an external mechanism, not by the GC, so they
    are counted as live throughout — which is why a graph dominated by many
    final outputs (a wide fan-out) shows w close to M and almost no
    Immediate advantage. The advantage of the Immediate policy is the ratio
    M / w.
    """
    status: dict[int, TaskStatus] = {t: TaskStatus.DEFINED for t in wf.tasks}
    live: set[int] = set()
    peak = 0
    total = len(wf.datasets)

    for tid in sorted(wf.tasks):
        task = wf.tasks[tid]
        for out_id in task.output_ids:
            live.add(out_id)
        status[tid] = TaskStatus.FINISHED
        # Immediate sweep: drop intermediate datasets whose consumers are all
        # terminal (finals are kept — offloaded externally).
        for ds_id in list(live):
            if wf.datasets[ds_id].is_boundary:
                continue
            consumers = wf.consumers(ds_id)
            # Vacuous-∀ matches can_delete / eq. (1): a non-boundary dataset
            # with no consumers is freed (all([]) is True). The builders never
            # produce such dead-ends, so measured M/w is unchanged; this only
            # keeps the predicate identical to the article's.
            if all(status[c] in TERMINAL for c in consumers):
                live.discard(ds_id)
        peak = max(peak, len(live))

    return peak, total


# ---- admission-controlled backlog scenario ------------------------------


@dataclass
class BacklogResult:
    """Trace of a backlog run under one cleanup policy."""

    policy: str
    times: list[float]            # event-grid timestamps
    occupancy: list[float]        # buffer occupancy at each timestamp (≤ v_max)
    completed: list[float]        # cumulative completed chains at each timestamp
    concurrency: int              # chains admitted per scheduling round
    makespan: float               # time to drain the whole backlog


def simulate_backlog(
    policy: str,
    n_chains: int,
    chain_length: int,
    duration: float,
    size: float,
    v_max: float,
    final_offload_delay: float = 0.0,
    dt: float = 0.25,
) -> BacklogResult:
    """
    Drain a fixed backlog of ``n_chains`` chain workflows through a buffer
    of HARD capacity ``v_max``, under admission control.

    The buffer is a hard ceiling: occupancy never exceeds ``v_max``.
    This is the key correction over an unconstrained model — when the
    buffer is full the system does not overflow, it *waits* for space to
    be freed before admitting more work.

    Scheduling model (synchronized rounds)
    --------------------------------------
    The binding resource is the buffer, not CPU (workers are assumed
    plentiful). Admission is governed by each policy's *peak per-chain
    buffer footprint*:

      - Immediate: a chain ever holds at most one intermediate at a time
        (the previous output is freed as soon as the next task consumes
        it), so its peak footprint is ``size``.
      - Deferred: a chain accumulates all of its intermediates until it
        finishes, so its peak footprint is ``chain_length · size``.

    The number of chains that fit simultaneously is therefore

        C = floor(v_max / peak_footprint),

    which is ``v_max / size`` for Immediate but only
    ``v_max / (chain_length · size)`` for Deferred — a factor of
    ``chain_length`` fewer. Chains are processed in synchronized rounds of
    ``C`` chains; a round occupies the buffer for ``chain_length · duration``
    (all tasks run) plus ``final_offload_delay`` (the external publisher
    picks up the finals), after which the buffer clears and the next round
    is admitted. Both policies use the SAME round duration — so Deferred,
    admitting ``chain_length`` times fewer chains per round, needs that
    many more rounds to drain the same backlog.

    This synchronized-round model is a deliberate simplification: it makes
    the throughput ratio between the policies exactly visible and keeps
    the buffer ceiling provably respected (no overflow, no deadlock). A
    pipelined scheduler would raise both throughputs by a constant factor
    but leave their ratio — the subject of the comparison — unchanged
    (empirically verified: the imm/def throughput ratio is ~9.2 here under
    the M/G/C model of ``simulate_run`` and ~9.6 under these rounds, both
    ≈ L = 10).

    That invariance concerns the *scheduling discipline* (rounds vs.
    pipeline) and is separate from the admission *unit*: charging each chain
    its PEAK footprint (``size`` for Immediate, ``L*size`` for Deferred) is
    conservative for Deferred, whose true occupancy ramps 0,d,…,(L-1)d
    (time-average ≈ (L-1)/2·d, i.e. ~4.5d at L=10 vs. the 10d peak).
    Admitting by time-average occupancy instead would fit ~2x more Deferred
    chains and shrink the M/w advantage toward ~L/2. Peak reservation is the
    safe choice under a hard V_max (simultaneous peaks must not overflow);
    see the article's "peak vs. average footprint" remark.

    Returns
    -------
    BacklogResult with synchronized event-grid traces of occupancy and
    cumulative completions.
    """
    if policy not in Workflow.POLICIES:
        raise ValueError(f"policy must be one of {Workflow.POLICIES}")
    if n_chains < 1 or chain_length < 1:
        raise ValueError("n_chains and chain_length must be ≥ 1")
    if v_max <= 0 or size <= 0 or duration <= 0 or dt <= 0:
        raise ValueError("v_max, size, duration, dt must be positive")
    if final_offload_delay < 0:
        raise ValueError("final_offload_delay must be ≥ 0")

    L = chain_length
    tau = duration
    peak = size * (1.0 if policy == "immediate" else float(L))
    concurrency = max(1, int(v_max // peak))
    round_run = L * tau                       # all tasks of the round finish
    round_total = round_run + final_offload_delay
    n_rounds = math.ceil(n_chains / concurrency)

    times: list[float] = []
    occ: list[float] = []
    comp: list[float] = []
    completed = 0

    def per_chain_occupancy(e: float) -> float:
        """Buffer held by ONE chain, ``e`` seconds into its round."""
        if policy == "immediate":
            # Holds one intermediate from the first output (e ≥ tau) until
            # the final is offloaded; never more than one dataset at a time.
            return size if (tau <= e < round_total) else 0.0
        # deferred: accumulates one dataset per completed task.
        if e < round_run:
            held = min(int(e // tau), L)        # 0,1,…,L-1 completed outputs
            return size * held
        if e < round_total:
            return size                         # finals held until offload
        return 0.0

    for r in range(n_rounds):
        batch = min(concurrency, n_chains - r * concurrency)
        t0 = r * round_total
        n_steps = math.ceil(round_total / dt)   # hits e=round_total exactly once
        for s in range(n_steps + 1):
            e = min(s * dt, round_total)
            times.append(t0 + e)
            occ.append(batch * per_chain_occupancy(e))
            comp.append(completed + (batch if e >= round_run else 0))
        # Explicit pre-sweep peak sample for the Deferred staircase: at the
        # instant the last output appears the chain momentarily holds all L
        # datasets before the batch GC sweep fires.
        if policy == "deferred":
            times.append(t0 + round_run)
            occ.append(batch * size * L)
            comp.append(completed + batch)
        completed += batch

    makespan = n_rounds * round_total
    return BacklogResult(
        policy=policy,
        times=times,
        occupancy=occ,
        completed=comp,
        concurrency=concurrency,
        makespan=makespan,
    )


# ---- single-run drain scenario ------------------------------------------


@dataclass
class RunResult:
    """Outcome of draining one data-taking run under one cleanup policy."""

    policy: str
    concurrency: int               # max chains the buffer lets run at once
    n_chains: int                  # chains generated during the run
    finish_time: float             # absolute time the last chain completes (s)
    required_gap: float            # idle time needed after the run ends (s)
    keeps_up: bool                 # True if processing kept pace with arrival
    grid_t: list[float]            # sample timestamps (s)
    backlog: list[float]           # unprocessed chains at each sample
    run_duration: float            # length of the data-taking run (s)


def simulate_run(
    policy: str,
    arrival_rate: float,
    run_duration: float,
    chain_length: int,
    stage_time_min: float,
    stage_time_max: float,
    size: float,
    v_max: float,
    *,
    seed: int = 42,
    n_grid: int = 600,
) -> RunResult:
    """
    Simulate ONE data-taking run and its drain-out under one cleanup policy.

    During ``[0, run_duration]`` data chunks arrive at ``arrival_rate``
    chains per second (one chunk = one chain workflow). After the run ends
    no new data arrives and the system keeps processing until the backlog
    is empty. We report when the last chain finishes and how much idle time
    (the inter-run gap) that requires.

    Resource model
    --------------
    The buffer is the binding resource: the farm is assumed large enough not
    to be the bottleneck, so the number of chains that can run concurrently
    is set by how many fit in ``v_max`` under each policy's peak per-chain
    footprint:

        Immediate: footprint = size       -> C = v_max / size
        Deferred:  footprint = L * size    -> C = v_max / (L * size)

    This is the PEAK footprint, held for the whole service time. A Deferred
    chain's true occupancy ramps 0,d,…,(L-1)d (time-average ≈ (L-1)/2·d), so
    reserving L*size throughout is a conservative (worst-case) admission rule
    — the correct one under a hard v_max, but it makes M/w = L an upper bound;
    the time-averaged advantage is ~L/2 (see the article's peak-vs-average
    remark).

    The system is then an M/G/C queue with ``C`` servers: each admitted
    chain occupies one buffer reservation for its whole processing time
    (the sum of ``chain_length`` per-stage durations, each drawn uniformly
    from ``[stage_time_min, stage_time_max]``) and frees it on completion.
    A smaller real farm would cap both policies at the farm size and shrink
    the gap between them proportionally; the buffer-bound regime here
    isolates the effect of the cleanup policy.

    Returns
    -------
    RunResult with the backlog-over-time trace and the required inter-run gap.
    """
    if policy not in Workflow.POLICIES:
        raise ValueError(f"policy must be one of {Workflow.POLICIES}")
    if arrival_rate <= 0 or run_duration <= 0 or chain_length < 1:
        raise ValueError("arrival_rate, run_duration > 0 and chain_length ≥ 1")
    if not (0 < stage_time_min <= stage_time_max):
        raise ValueError("require 0 < stage_time_min ≤ stage_time_max")
    if v_max <= 0 or size <= 0:
        raise ValueError("v_max and size must be positive")

    rng = random.Random(seed)
    L = chain_length
    peak = size * (1.0 if policy == "immediate" else float(L))
    concurrency = max(1, int(v_max // peak))
    n_chains = int(arrival_rate * run_duration)
    arr_interval = 1.0 / arrival_rate

    def service_time() -> float:
        # total processing time of one chain = sum of L per-stage durations
        return sum(
            rng.uniform(stage_time_min, stage_time_max) for _ in range(L)
        )

    busy: list[float] = []          # min-heap of in-flight completion times
    waiting = 0                     # admitted-queue length (arrived, not started)
    completed = 0
    completion_times: list[float] = []
    next_arrival = 0
    t = 0.0
    INF = float("inf")

    while completed < n_chains:
        t_arr = next_arrival * arr_interval if next_arrival < n_chains else INF
        t_cmp = busy[0] if busy else INF

        if t_arr <= t_cmp:
            t = t_arr
            if len(busy) < concurrency:
                heapq.heappush(busy, t + service_time())
            else:
                waiting += 1
            next_arrival += 1
        else:
            t = heapq.heappop(busy)
            completed += 1
            completion_times.append(t)
            if waiting > 0:
                waiting -= 1
                heapq.heappush(busy, t + service_time())

    finish_time = completion_times[-1]
    required_gap = max(0.0, finish_time - run_duration)
    # "keeps up" = the buffer-limited throughput mu is at least the arrival
    # rate, so no unbounded backlog forms during the run (the condition the
    # field name actually means: mu = C / (L * mean-stage-time) >= lambda).
    tau_mean = 0.5 * (stage_time_min + stage_time_max)
    mu = concurrency / (L * tau_mean)
    keeps_up = mu >= arrival_rate

    # backlog(t) = arrived(t) − completed(t) on a regular grid
    grid_t: list[float] = []
    backlog: list[float] = []
    horizon = finish_time * 1.02
    for k in range(n_grid + 1):
        tk = horizon * k / n_grid
        arrived = min(n_chains, math.floor(arrival_rate * min(tk, run_duration)))
        done = bisect_right(completion_times, tk)
        grid_t.append(tk)
        backlog.append(float(arrived - done))

    return RunResult(
        policy=policy,
        concurrency=concurrency,
        n_chains=n_chains,
        finish_time=finish_time,
        required_gap=required_gap,
        keeps_up=keeps_up,
        grid_t=grid_t,
        backlog=backlog,
        run_duration=run_duration,
    )


__all__ = [
    "TaskStatus",
    "TERMINAL",
    "Dataset",
    "Task",
    "Workflow",
    "build_chain",
    "build_fanout_tree",
    "measure_footprints",
    "BacklogResult",
    "simulate_backlog",
    "RunResult",
    "simulate_run",
]
