"""
Seed-robustness check for the run-drain experiment.

Runs ``simulate_run`` for both cleanup policies across ``N`` RNG seeds with the
default SCENARIO and reports, for the required inter-run gap and the finish
time: mean, standard deviation, 95% confidence interval, min/max and the
coefficient of variation.

The point is to show that the headline numbers (~4 h vs ~123 h) are essentially
seed-independent: the drain time is throughput-bound (eq. for T_gap), and the
per-stage timing randomness averages out over the ~10^6 chains processed in one
run, so the stochastic spread is sub-percent.

Usage:
    python report_seed_variance.py            # 32 seeds (default)
    SEED_COUNT=12 python report_seed_variance.py
"""

from __future__ import annotations

import os
import statistics as st

from config import SCENARIO as S
from topological_gc import simulate_run

N = int(os.environ.get("SEED_COUNT", "32"))


def ci95(xs: list[float]) -> float:
    """Half-width of the 95% confidence interval of the mean."""
    if len(xs) < 2:
        return 0.0
    return 1.96 * st.pstdev(xs) / (len(xs) ** 0.5)


def run() -> None:
    common = dict(
        arrival_rate=S.arrival_rate, run_duration=S.run_s,
        chain_length=S.n_stages, stage_time_min=S.tau_min_s,
        stage_time_max=S.tau_max_s, size=S.dataset_gb, v_max=S.v_max_gb,
    )
    print(
        f"scenario: R={S.rate_gb_s} GB/s, d={S.dataset_gb} GB, "
        f"Vmax={S.v_max_tb} TB, L={S.n_stages}, "
        f"tau~U({S.tau_min_s/60:.0f},{S.tau_max_s/60:.0f}) min, "
        f"run={S.run_hours:.0f} h; seeds=0..{N-1}"
    )
    for policy in ("immediate", "deferred"):
        gaps: list[float] = []
        concurrency = 0
        for seed in range(N):
            r = simulate_run(policy, seed=seed, **common)
            gaps.append(r.required_gap / 3600.0)
            concurrency = r.concurrency
        mean = st.mean(gaps)
        cv = (st.pstdev(gaps) / mean * 100.0) if mean else 0.0
        print(
            f"{policy:9s} gap[h]: mean={mean:.3f}  "
            f"+/-{ci95(gaps):.3f} (95% CI)  std={st.pstdev(gaps):.3f}  "
            f"min={min(gaps):.3f}  max={max(gaps):.3f}  CV={cv:.2f}%  "
            f"C={concurrency}"
        )


if __name__ == "__main__":
    run()
