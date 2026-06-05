"""
Central configuration for the Topological-GC simulation.

Every scenario constant lives here and can be overridden from the
environment, so anyone can plug in their own workload without touching the
code:

    RATE_GB_S=8 VMAX_TB=20 L_STAGES=6 python plot_run_drain.py

The numbers below are a generic high-rate stream-processing scenario; they
are not tied to any specific experiment. Override them to match your own
system and read the optimal operating point off the generated figures.

All times are seconds, all sizes are gigabytes unless noted.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _f(name: str, default: float) -> float:
    """Read a float from the environment, falling back to ``default``."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"env {name}={raw!r} is not a number") from exc


def _i(name: str, default: int) -> int:
    """Read an int from the environment, falling back to ``default``."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"env {name}={raw!r} is not an integer") from exc


@dataclass(frozen=True)
class Scenario:
    """A fully resolved workload scenario (all units: seconds, GB)."""

    rate_gb_s: float          # input data rate
    dataset_gb: float         # size of one chunk / dataset
    run_hours: float          # length of one data-taking run
    v_max_tb: float           # buffer capacity (hard ceiling)
    n_stages: int             # processing stages per chain (L)
    tau_min_s: float          # per-stage processing time, lower bound
    tau_max_s: float          # per-stage processing time, upper bound
    backlog_chains: int       # backlog size for the throughput demo
    seed: int                 # RNG seed for reproducibility

    # ---- derived quantities ----
    @property
    def run_s(self) -> float:
        return self.run_hours * 3600.0

    @property
    def v_max_gb(self) -> float:
        return self.v_max_tb * 1000.0

    @property
    def arrival_rate(self) -> float:
        """λ — chains per second."""
        return self.rate_gb_s / self.dataset_gb

    @property
    def tau_mean_s(self) -> float:
        return 0.5 * (self.tau_min_s + self.tau_max_s)

    def concurrency(self, policy: str) -> int:
        """Chains that fit in the buffer at once under a policy."""
        footprint = self.dataset_gb * (
            1.0 if policy == "immediate" else float(self.n_stages)
        )
        return max(1, int(self.v_max_gb // footprint))


def load_scenario() -> Scenario:
    """Build the scenario from defaults, overridden by environment vars."""
    return Scenario(
        rate_gb_s=_f("RATE_GB_S", 20.0),       # 20 GB/s input stream
        dataset_gb=_f("DATASET_GB", 1.0),      # 1 GB per chunk
        run_hours=_f("RUN_HOURS", 10.0),       # 10-hour run
        v_max_tb=_f("VMAX_TB", 50.0),          # 50 TB buffer
        n_stages=_i("L_STAGES", 10),           # 10 processing stages
        tau_min_s=_f("TAU_MIN_S", 60.0),       # 1 min / stage (lower)
        tau_max_s=_f("TAU_MAX_S", 600.0),      # 10 min / stage (upper)
        backlog_chains=_i("BACKLOG_CHAINS", 320),
        seed=_i("SEED", 42),
    )


# Module-level singleton for convenience: `from config import SCENARIO`.
SCENARIO = load_scenario()
