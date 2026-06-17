"""
Baseline comparison: semantic cleanup (Topological GC / CanDelete) vs. naive
reference counting vs. a fixed time-to-live (TTL), under a workload that
INCLUDES task failures.

This is the experiment the article's motivation calls for. The intro argues
that freeing a dataset by the *terminal status* of its consumers beats
reference-based and time-based reclamation precisely because a job may FAIL;
here that argument is measured rather than asserted. A stream of independent
linear chains (one external trigger -> L stages) arrives at a constant rate;
a fraction of chains has one stage fail (its downstream stages are cancelled).
We track total buffer occupancy over time under three reclamation policies.

Policies (each decides when an intermediate dataset ``d`` may be freed):

  - semantic (CanDelete):  free when every consumer of ``d`` is TERMINAL
        (FINISHED, FAILED or CANCELLED) — the article's criterion (eq. 1).
  - refcount:              free only when the consumer SUCCEEDS (FINISHED);
        a failed/cancelled consumer never releases its input -> the dataset is
        stranded forever. This is the "naive counter" the intro warns about.
  - ttl:                   free at ``created + ttl`` regardless of consumer
        status — not tied to completion semantics.

Final (boundary) datasets are offloaded by an external mechanism at
``created + offload`` under every policy, so they do not differentiate the
policies; the difference lives entirely in how intermediates are reclaimed.

Per-stage time is deterministic here (each stage takes ``tau``), so the buffer
occupancy of every dataset is a closed interval ``[created, freed)`` and the
trace is computed exactly by counting live intervals on a time grid — no RNG.

The headline effect: a failed consumer strands exactly one dataset under
``refcount`` (its input), so stranded data accumulates at the failure rate and
the buffer grows without bound, eventually breaching ``V_max``; ``semantic``
frees that dataset the instant the consumer fails and stays bounded; ``ttl``
stays bounded but elevated (it holds every dataset for ``ttl`` rather than
until it is actually consumed), and is unsafe if shortened below the time a
dataset is still needed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

INF = float("inf")


@dataclass
class BaselineTrace:
    """Buffer-occupancy traces for the three reclamation policies."""

    times: list[float]
    occ_semantic: list[float]
    occ_refcount: list[float]
    occ_ttl: list[float]
    v_max: float
    ttl: float
    n_chains: int
    n_failed: int


def _chain_intervals(
    t0: float, L: int, tau: float, offload: float, ttl: float,
    failed: bool, fail_stage: int,
) -> list[tuple[float, float, float, float]]:
    """
    Intervals ``(created, freed_semantic, freed_refcount, freed_ttl)`` for each
    dataset of one chain that arrives at ``t0``.

    Healthy chain: stages 1..L succeed; intermediates d_1..d_{L-1} are each
    consumed (and freed) by the next stage, d_L is the boundary final.
    Failed chain: stage ``fail_stage`` fails, so only d_1..d_{fail_stage-1}
    are ever produced; d_{fail_stage-1} (the failed stage's input) is the one
    that refcount strands.
    """
    out: list[tuple[float, float, float, float]] = []
    if not failed:
        for i in range(1, L):                       # d_1 .. d_{L-1}
            created = t0 + i * tau
            consumed = t0 + (i + 1) * tau            # next stage FINISHES
            out.append((created, consumed, consumed, created + ttl))
        created = t0 + L * tau                       # d_L: boundary final
        off = created + offload
        out.append((created, off, off, off))         # offloaded externally
    else:
        f = fail_stage
        for i in range(1, f):                        # d_1 .. d_{f-1}
            created = t0 + i * tau
            if i < f - 1:                            # consumer succeeds
                consumed = t0 + (i + 1) * tau
                out.append((created, consumed, consumed, created + ttl))
            else:                                    # d_{f-1}: input to stage f
                fail_t = t0 + f * tau                # stage f FAILS here
                #  semantic frees (consumer terminal); refcount strands (INF);
                #  ttl frees on age.
                out.append((created, fail_t, INF, created + ttl))
        # downstream stages cancelled -> no further datasets, no final
    return out


def simulate_baselines(
    L: int = 10,
    tau: float = 1.0,
    size: float = 1.0,
    n_chains: int = 130,
    arr_interval: float = 1.0,
    fail_period: int = 3,
    fail_stage: int = 5,
    ttl: float = 3.0,
    offload: float = 2.0,
    v_max: float = 40.0,
    dt: float = 0.5,
) -> BaselineTrace:
    """Compute buffer-occupancy traces for the three policies (see module doc)."""
    if not (2 <= fail_stage <= L):
        raise ValueError("require 2 <= fail_stage <= L (need an input to strand)")
    if min(tau, size, arr_interval, ttl, dt) <= 0 or v_max <= 0:
        raise ValueError("tau, size, arr_interval, ttl, dt, v_max must be > 0")

    sem: list[tuple[float, float]] = []
    rc: list[tuple[float, float]] = []
    tt: list[tuple[float, float]] = []
    n_failed = 0
    for k in range(n_chains):
        t0 = k * arr_interval
        failed = fail_period > 0 and k > 0 and k % fail_period == 0
        n_failed += int(failed)
        for created, fs, fr, ft in _chain_intervals(
            t0, L, tau, offload, ttl, failed, fail_stage
        ):
            sem.append((created, fs))
            rc.append((created, fr))
            tt.append((created, ft))

    horizon = (n_chains - 1) * arr_interval + L * tau + offload + 5.0
    n = int(math.ceil(horizon / dt))
    times: list[float] = []
    occ_s: list[float] = []
    occ_r: list[float] = []
    occ_t: list[float] = []
    for j in range(n + 1):
        t = j * dt
        times.append(t)
        occ_s.append(size * sum(1 for c, fd in sem if c <= t < fd))
        occ_r.append(size * sum(1 for c, fd in rc if c <= t < fd))
        occ_t.append(size * sum(1 for c, fd in tt if c <= t < fd))

    return BaselineTrace(
        times=times, occ_semantic=occ_s, occ_refcount=occ_r, occ_ttl=occ_t,
        v_max=v_max, ttl=ttl, n_chains=n_chains, n_failed=n_failed,
    )


def _first_breach(times: list[float], occ: list[float], v_max: float) -> float | None:
    for t, o in zip(times, occ):
        if o > v_max:
            return t
    return None


if __name__ == "__main__":
    tr = simulate_baselines()
    print(
        f"chains={tr.n_chains} (failed={tr.n_failed}, "
        f"{100*tr.n_failed/tr.n_chains:.0f}%), V_max={tr.v_max}, ttl={tr.ttl}"
    )
    for name, occ in (
        ("semantic", tr.occ_semantic),
        ("refcount", tr.occ_refcount),
        ("ttl     ", tr.occ_ttl),
    ):
        breach = _first_breach(tr.times, occ, tr.v_max)
        print(
            f"{name}: peak={max(occ):.0f}  final={occ[-1]:.0f}  "
            f"mean={sum(occ)/len(occ):.1f}  "
            f"breach V_max at t={breach if breach is not None else 'never'}"
        )
