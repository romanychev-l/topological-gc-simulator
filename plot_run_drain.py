"""
Single-run drain experiment.

One 10-hour data-taking run feeds chain workflows into a fixed 50 TB buffer
at 20 GB/s; after the run ends the system keeps processing until the backlog
is empty. We measure, for each cleanup policy:

  - when the last chain finishes (finish time), and
  - the idle gap that must separate consecutive runs so everything is
    processed before the next run starts.

Two panels:
  (a) unprocessed data (TB) over time — both policies, run window shaded,
      required inter-run gap marked.
  (b) required gap as a function of the (uncertain) per-stage processing
      time tau — turns the 1..10 min uncertainty into the x-axis.

Usage:
    python plot_run_drain.py

Outputs:
    outputs/run_drain.pdf
    outputs/run_drain.png
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from config import SCENARIO
from topological_gc import simulate_run

OUT_DIR = Path(__file__).parent / "outputs"
C_IMM = "#1f4e96"
C_DEF = "#b1361b"

# Scenario constants — defaults in config.py, overridable from the environment.
S = SCENARIO
RUN_HOURS = S.run_hours
V_MAX_TB = S.v_max_tb
L = S.n_stages
TAU_MIN_S = S.tau_min_s
TAU_MAX_S = S.tau_max_s
RUN_S = S.run_s
DATASET_GB = S.dataset_gb
LAMBDA = S.arrival_rate          # chains/s


def required_gap_analytic(concurrency: float, tau_mean: float) -> float:
    """
    Closed-form inter-run gap at mean per-stage time ``tau_mean``.

    Buffer-limited throughput is mu = C / (L * tau). The wall-time to push
    all N = lambda * run chains through C saturated servers is N / mu; the
    gap is whatever part of that spills past the run window:

        gap = max(0, N / mu - run).

    Below the keep-up threshold (mu >= lambda) the run keeps pace and the
    gap collapses to ~0 (only the small standing in-flight set drains).
    """
    n = LAMBDA * RUN_S
    mu = concurrency / (L * tau_mean)
    return max(0.0, n / mu - RUN_S)


def run() -> None:
    common = dict(
        arrival_rate=LAMBDA, run_duration=RUN_S, chain_length=L,
        stage_time_min=TAU_MIN_S, stage_time_max=TAU_MAX_S,
        size=DATASET_GB, v_max=S.v_max_gb, seed=S.seed,
    )
    imm = simulate_run("immediate", **common)
    dfr = simulate_run("deferred", **common)

    def to_tb(chains: list[float]) -> list[float]:
        return [c * DATASET_GB / 1000.0 for c in chains]

    def hrs(seconds: list[float]) -> list[float]:
        return [s / 3600.0 for s in seconds]

    fig, (ax_a, ax_b) = plt.subplots(2, 1, figsize=(10.0, 7.8))

    # ---------------- panel (a): backlog over time --------------------
    ax_a.plot(hrs(dfr.grid_t), to_tb(dfr.backlog), color=C_DEF,
              linewidth=2.0, label="Отложенная (Deferred)")
    ax_a.plot(hrs(imm.grid_t), to_tb(imm.backlog), color=C_IMM,
              linewidth=2.0, label="Событийная (Immediate)")

    ax_a.axvspan(0, RUN_HOURS, color="grey", alpha=0.10)
    ax_a.text(RUN_HOURS / 2, ax_a.get_ylim()[1] * 0.93,
              f"сеанс приёма данных, {RUN_HOURS:.0f} ч", color="dimgrey",
              fontsize=9, ha="center", va="top")

    for res, color, name in ((imm, C_IMM, "Событийная"),
                             (dfr, C_DEF, "Отложенная")):
        fin_h = res.finish_time / 3600.0
        gap_h = res.required_gap / 3600.0
        ax_a.axvline(fin_h, color=color, linestyle="--", linewidth=0.9,
                     alpha=0.7)
        ax_a.annotate(
            f"{name}: перерыв ≈ {gap_h:.0f} ч",
            xy=(fin_h, 0), xytext=(fin_h, ax_a.get_ylim()[1] * (0.55 if name == "Отложенная" else 0.30)),
            color=color, fontsize=9, ha="right", va="center",
            rotation=90,
        )

    ax_a.set_xlabel("Время от начала сеанса, ч")
    ax_a.set_ylabel("Накопленный остаток (поступило − обработано), ТБ")
    ax_a.set_title(
        f"(а) Разбор накопленного остатка после одного {RUN_HOURS:.0f}-часового сеанса "
        f"($V_{{\\max}}={V_MAX_TB:.0f}$ ТБ, $L={L}$, "
        f"$\\tau\\sim U({TAU_MIN_S/60:.0f},{TAU_MAX_S/60:.0f})$ мин)"
    )
    ax_a.spines["top"].set_visible(False)
    ax_a.spines["right"].set_visible(False)
    ax_a.set_xlim(0, dfr.finish_time / 3600.0 * 1.02)
    ax_a.set_ylim(bottom=0)
    ax_a.legend(loc="upper right", frameon=False)

    # ---------------- panel (b): required gap vs tau ------------------
    floor_h = 0.05   # display floor so the keep-up region is drawable on log-y
    taus = [TAU_MIN_S + (TAU_MAX_S - TAU_MIN_S) * k / 200 for k in range(201)]
    gap_imm = [max(floor_h, required_gap_analytic(imm.concurrency, t) / 3600.0)
               for t in taus]
    gap_dfr = [max(floor_h, required_gap_analytic(dfr.concurrency, t) / 3600.0)
               for t in taus]
    tau_min = [t / 60.0 for t in taus]

    ax_b.plot(tau_min, gap_dfr, color=C_DEF, linewidth=2.0, label="Отложенная (Deferred)")
    ax_b.plot(tau_min, gap_imm, color=C_IMM, linewidth=2.0, label="Событийная (Immediate)")

    # mark the keep-up threshold for Immediate (gap collapses to ~0)
    tau_thr_min = (imm.concurrency / LAMBDA / L) / 60.0
    if 1.0 <= tau_thr_min <= 10.0:
        ax_b.axvline(tau_thr_min, color=C_IMM, linestyle=":", linewidth=0.8,
                     alpha=0.6)
        ax_b.text(tau_thr_min - 0.1, floor_h * 1.5,
                  "Событийная\nуспевает\nв реальном\nвремени",
                  color=C_IMM, fontsize=7.5, ha="right", va="bottom")

    # mark the simulated mean-tau points
    tau_mean_min = S.tau_mean_s / 60.0
    ax_b.scatter([tau_mean_min], [imm.required_gap / 3600.0],
                 color=C_IMM, zorder=5, s=30)
    ax_b.scatter([tau_mean_min], [dfr.required_gap / 3600.0],
                 color=C_DEF, zorder=5, s=30)

    ax_b.axhline(1.0, color="grey", linestyle=":", linewidth=0.9)
    ax_b.text(tau_min[-1], 1.0, " перерыв 1 ч", color="dimgrey",
              fontsize=8, ha="right", va="bottom")

    ax_b.set_yscale("log")
    ax_b.set_xlabel("Время обработки одной стадии $\\tau$, мин")
    ax_b.set_ylabel("Необходимый перерыв между сеансами, ч (лог. шкала)")
    ax_b.set_title(
        f"(б) Необходимый перерыв между сеансами в зависимости от $\\tau$ "
        f"(точки — стохастический прогон при "
        f"$\\bar\\tau={tau_mean_min:.1f}$ мин)"
    )
    ax_b.spines["top"].set_visible(False)
    ax_b.spines["right"].set_visible(False)
    ax_b.set_xlim(TAU_MIN_S / 60.0, TAU_MAX_S / 60.0)
    ax_b.legend(loc="center right", frameon=False)

    plt.tight_layout()

    OUT_DIR.mkdir(exist_ok=True)
    pdf = OUT_DIR / "run_drain.pdf"
    png = OUT_DIR / "run_drain.png"
    fig.savefig(pdf)
    fig.savefig(png, dpi=200)
    print(f"saved {pdf.relative_to(Path.cwd())}")
    print(f"saved {png.relative_to(Path.cwd())}")
    print(f"Immediate: C={imm.concurrency}, finish={imm.finish_time/3600:.1f} h,"
          f" gap={imm.required_gap/3600:.1f} h")
    print(f"Deferred:  C={dfr.concurrency}, finish={dfr.finish_time/3600:.1f} h,"
          f" gap={dfr.required_gap/3600:.1f} h")


if __name__ == "__main__":
    run()
