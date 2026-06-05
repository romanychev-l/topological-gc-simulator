# Topological GC Simulator

Reference implementation accompanying the article

> **«Детерминированный метод управления жизненным циклом промежуточных
> данных в DAG-ориентированных системах многокаскадной обработки»**
> Л.Р. Романычев, 2026.

A small, self-contained discrete-event simulator that models a workflow
management system (WfMS) and reproduces the buffer-occupancy curves
shown in the article under the two cleanup policies introduced there.

## What it does

The simulator implements the article's formal model one-to-one:

```
Consumers(d) = { j ∈ J : (d, j) ∈ E }

CanDelete(d) ⇔ ¬IsBoundary(d) ∧ ∀ j ∈ Consumers(d): status(j) ∈ T,
    where T = {FINISHED, FAILED, CANCELLED}.

Active(G) = { j ∈ J : status(j) ∉ T }.
```

…and two cleanup policies on top of it:

- **Immediate** — every dataset satisfying `CanDelete` is freed as soon
  as the triggering consumer transitions to a terminal status.
- **Deferred** — intermediate datasets are held until `Active(G) = ∅`,
  then released in a single batch sweep.

Boundary datasets (trigger inputs and final outputs) are exempt from
auto-deletion; an external publisher picks up final outputs after a
configurable `final_offload_delay`.

The headline experiment drains a fixed backlog of chain workflows
through a buffer of **hard capacity** `V_max` under **admission
control**: the buffer is a ceiling that is never exceeded — when it is
full, the system waits for space rather than overflowing. The policies
are then compared on **throughput** (how fast the backlog drains), which
is where they differ by a factor of the chain length.

## Quick start

With [uv](https://docs.astral.sh/uv/) (recommended — no manual venv, deps
resolved from `pyproject.toml`/`uv.lock` on first run):

```bash
uv run plot_run_drain.py           # headline: single-run drain + inter-run gap
uv run plot_policies.py            # supporting: occupancy / throughput
uv run plot_branching.py           # advantage M/w vs graph shape (branching)
```

Or with a classic pip virtualenv:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python plot_run_drain.py
```

Figures land in `outputs/run_drain.{pdf,png}` and
`outputs/policies_throughput.{pdf,png}`.

## Plug in your own workload

All scenario constants live in `config.py` and can be overridden from the
environment — no code changes needed. Set your own numbers and read the
optimal operating point (the keep-up threshold and required gap) straight
off the regenerated figures:

```bash
RATE_GB_S=8 VMAX_TB=20 L_STAGES=6 TAU_MIN_S=120 TAU_MAX_S=300 \
    uv run plot_run_drain.py
```

| Variable | Meaning | Default |
|---|---|---|
| `RATE_GB_S` | input data rate, GB/s | 20 |
| `DATASET_GB` | size of one chunk / dataset, GB | 1 |
| `RUN_HOURS` | length of one data-taking run, h | 10 |
| `VMAX_TB` | buffer capacity, TB (hard ceiling) | 50 |
| `L_STAGES` | processing stages per chain (L) | 10 |
| `TAU_MIN_S` | per-stage time, lower bound, s | 60 |
| `TAU_MAX_S` | per-stage time, upper bound, s | 600 |
| `BACKLOG_CHAINS` | backlog size for the throughput demo | 320 |
| `SEED` | RNG seed (reproducibility) | 42 |

## Files

| File | Role |
|---|---|
| `config.py` | All scenario constants in one place, each overridable from the environment. No external dependencies. |
| `topological_gc.py` | Model (`Task`, `Dataset`, `Workflow`), the `CanDelete` predicate, the `Active(G)` set, the single-workflow engine, graph builders (`build_chain`, `build_fanout_tree`), the footprint measurement `measure_footprints` (cut-width `w` and total datasets `M`), `simulate_backlog` and `simulate_run`. No external dependencies. |
| `plot_run_drain.py` | **Headline experiment.** One 10 h run feeding a fixed buffer; reports backlog-over-time and the idle gap each policy needs between runs. Depends on `matplotlib`. |
| `plot_branching.py` | Advantage `M/w` vs graph shape: measures `w`, `M` for a linear chain and several fan-out trees, showing the order-`L` advantage holds for deep non-multiplying pipelines and collapses toward 1 for data-multiplying trees. Depends on `matplotlib`. |
| `plot_policies.py` | Supporting two-panel occupancy / throughput comparison. Depends on `matplotlib`. |
| `pyproject.toml` / `uv.lock` | Project metadata and locked dependencies for `uv`. |
| `requirements.txt` | Pinned dependency list for the pip fallback. |
| `outputs/` | Generated figures (kept out of version control). |

## Headline experiment: one run + inter-run gap

`plot_run_drain.py` models a single data-taking run of a representative
high-rate stream (units physical, not labelled SPD):

| Parameter | Value |
|---|---|
| input rate | 20 GB/s |
| dataset / chunk | 1 GB → 20 chains/s |
| run length | 10 h → 720 000 chains (720 TB) |
| buffer `V_max` | 50 TB (hard) |
| stages per chain `L` | 10 |
| per-stage time `τ` | Uniform(1, 10) min |

The buffer is the binding resource (the farm is assumed large enough not
to bottleneck), so concurrency is `C = V_max / (footprint)`: 50 000 chains
for Immediate (footprint 1 dataset) vs 5 000 for Deferred (footprint `L`).
The system is an M/G/C queue; after the run ends it drains until empty.

Result at `τ̄ = 5.5 min`:

| Policy | Concurrency | Finish | **Inter-run gap** |
|---|---|---|---|
| Immediate | 50 000 | 14.4 h | **≈ 4 h** |
| Deferred | 5 000 | 132.9 h | **≈ 123 h (≈ 5 days)** |

Neither keeps pace in real time at `τ̄ = 5.5 min`, but Immediate's deficit
clears in ~4 h while Deferred needs ~5 days between 10 h runs — operationally
infeasible. Immediate keeps up entirely (gap → 0) once `τ ≤ 4.2 min`;
Deferred would need `τ ≤ 25 s`. A smaller real farm caps both policies and
shrinks the gap between them proportionally.

## What the figure shows

The default scenario in `plot_policies.py`:

| Parameter | Value | Meaning |
|---|---|---|
| `n_chains` | 320 | size of the backlog to drain |
| `chain_length` (L) | 10 | tasks per chain |
| `duration` (τ) | 5 s | duration of each task |
| `dataset_size` | 1 | size of every dataset (abstract unit) |
| `final_offload_delay` | 5 s | external publisher picks up finals after δ |
| `v_max` | 80 | **hard** buffer capacity |

The buffer is a hard ceiling: occupancy never exceeds `v_max`. Admission
is governed by each policy's peak per-chain buffer footprint:

- **Immediate** holds at most one intermediate per chain at a time
  (footprint = `size`), so `C = v_max / size = 80` chains fit at once.
- **Deferred** holds *all* of a chain's intermediates until it finishes
  (footprint = `L · size`), so only `C = v_max / (L · size) = 8` chains
  fit.

Both policies run synchronized rounds of the same duration, so Deferred —
admitting `L` times fewer chains per round — needs `L` times more rounds
to drain the same backlog:

| Policy | Concurrency `C` | Makespan |
|---|---|---|
| Immediate | 80 | 220 s |
| Deferred | 8 | 2200 s |

The two panels:

- **(a) Occupancy** — both traces stay at or below `v_max`; the ceiling
  is never breached. Immediate packs the buffer to `v_max` densely and
  recycles it per task; Deferred ramps up to `v_max` as one round's
  intermediates accumulate, then clears in a single batch sweep.
- **(b) Throughput** — cumulative completed chains. Immediate drains the
  backlog ~`L`× faster. This is the payoff: the same hard buffer yields
  an order-of-magnitude more useful work under the Immediate policy,
  because eager cleanup frees space for far more concurrent chains.

## Design notes

The implementation is intentionally minimal — a few hundred lines of
pure Python — so it reads as a direct transcription of the formal
definitions in the article. It is **not** a production scheduler:

- Time is virtual; tasks do not consume real CPU. The engine advances a
  clock by each task's nominal duration.
- The binding resource is the **buffer**, not CPU: workers are assumed
  plentiful, so the number of concurrent chains is limited only by how
  many fit in `v_max`. This isolates the cleanup-policy effect, which is
  the subject of the comparison.
- The buffer ceiling `v_max` **is** enforced — occupancy never exceeds
  it. Admission control reserves each chain's peak footprint so the
  ceiling is provably respected and the schedule cannot deadlock.
- Chains are processed in **synchronized rounds**: a deliberate
  simplification that makes the throughput ratio between the policies
  exactly visible. A pipelined scheduler would raise both throughputs by
  a constant factor but leave their ratio — the article's main claim —
  unchanged.
- The `Workflow.simulate` method (used to build the single-chain TikZ
  shape illustration `figures/fig_policies_preview.tex`) materialises a
  task's output slightly before it transitions to `FINISHED`, modelling
  the brief write-overlap; the macro `simulate_backlog` model abstracts
  this away.

## Extending the simulator

The model is structured so the following extensions are local changes:

- **Branching DAGs.** Add `add_task(..., output_ids=[i, j, ...])` calls;
  the algorithm code is unchanged. `build_chain` is just one builder.
- **Failures.** Set `task.status = TaskStatus.FAILED` mid-simulation and
  call `can_delete` — terminal-status semantics already cover
  `FAILED`/`CANCELLED`.
- **Stochastic durations / sizes.** Pass distribution-sampled values
  into `add_task` / `add_dataset` instead of constants.
- **Comparison with TTL baselines.** Add a `simulate_ttl(workflow, ttl)`
  alternative engine that frees datasets `ttl` seconds after creation
  regardless of consumer status.

## License

MIT — see [`LICENSE`](LICENSE).
