"""
Generate the buffer-occupancy / throughput comparison used in the article:
draining a fixed backlog of chain workflows through a HARD-capacity buffer
under admission control, for the Immediate and Deferred cleanup policies.

Two panels:
  (a) buffer occupancy over time — both policies stay at or below V_max;
      the capacity is a hard ceiling that is never exceeded.
  (b) cumulative completed chains — Immediate drains the backlog ~L times
      faster, because freeing intermediates eagerly lets it keep far more
      chains in the buffer at once.

Usage:
    python plot_policies.py

Outputs:
    outputs/policies_throughput.pdf
    outputs/policies_throughput.png
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from config import SCENARIO
from topological_gc import simulate_backlog

OUT_DIR = Path(__file__).parent / "outputs"

C_IMM = "#1f4e96"
C_DEF = "#b1361b"


def run() -> None:
    # ----------------------------------------------------------------
    # Didactic "shape" demo on small abstract units (read dataset_size = 1
    # as one buffer slot). Only the chain length L is taken from the shared
    # config so both figures agree on it; the rest stay small so the
    # round-by-round occupancy shape is legible.
    # ----------------------------------------------------------------
    n_chains = SCENARIO.backlog_chains
    chain_length = SCENARIO.n_stages   # L: tasks per chain
    duration = 5.0                 # s per task -> chain runtime = 50 s
    dataset_size = 1.0
    final_offload_delay = 5.0      # external publisher pickup delay
    v_max = 8.0 * chain_length     # HARD buffer capacity (8 Deferred chains)

    imm = simulate_backlog(
        policy="immediate", n_chains=n_chains, chain_length=chain_length,
        duration=duration, size=dataset_size, v_max=v_max,
        final_offload_delay=final_offload_delay,
    )
    dfr = simulate_backlog(
        policy="deferred", n_chains=n_chains, chain_length=chain_length,
        duration=duration, size=dataset_size, v_max=v_max,
        final_offload_delay=final_offload_delay,
    )

    speedup = dfr.makespan / imm.makespan

    fig, (ax_o, ax_c) = plt.subplots(
        2, 1, figsize=(10.0, 7.4), height_ratios=[1.0, 1.0]
    )

    # ---------------- panel (a): occupancy ----------------------------
    # zoom to the first few rounds so the shape is legible
    zoom_t = 3.2 * (chain_length * duration + final_offload_delay)
    ax_o.step(imm.times, imm.occupancy, where="post",
              color=C_IMM, linewidth=1.8, label="Immediate")
    ax_o.step(dfr.times, dfr.occupancy, where="post",
              color=C_DEF, linewidth=1.8, label="Deferred")

    ax_o.axhline(v_max, color="grey", linestyle="--", linewidth=1.0)
    ax_o.fill_between([0, zoom_t], v_max, v_max * 1.18,
                      color="grey", alpha=0.10, zorder=0)
    ax_o.text(zoom_t * 0.5, v_max * 1.09,
              "недопустимая зона (переполнение буфера)",
              color="dimgrey", fontsize=8.5, ha="center", va="center")
    ax_o.text(zoom_t * 0.995, v_max, r" $V_{\max}$",
              color="dimgrey", fontsize=10, ha="right", va="bottom")

    ax_o.set_xlim(0, zoom_t)
    ax_o.set_ylim(0, v_max * 1.18)
    ax_o.set_xlabel("Время $t$, c")
    ax_o.set_ylabel("Занятость буфера, усл. ед.")
    ax_o.set_title(
        "(а) Занятость буфера: жёсткий потолок $V_{\\max}$ соблюдается обеими политиками"
    )
    ax_o.spines["top"].set_visible(False)
    ax_o.spines["right"].set_visible(False)
    ax_o.legend(loc="lower right", frameon=False, ncol=2)

    # ---------------- panel (b): cumulative throughput ----------------
    ax_c.step(imm.times, imm.completed, where="post",
              color=C_IMM, linewidth=2.0, label="Immediate")
    ax_c.step(dfr.times, dfr.completed, where="post",
              color=C_DEF, linewidth=2.0, label="Deferred")

    ax_c.axhline(n_chains, color="grey", linestyle=":", linewidth=0.8)
    ax_c.text(dfr.makespan * 0.5, n_chains + 6,
              f"вся нагрузка: {n_chains} цепочек",
              color="dimgrey", fontsize=9, ha="center")

    # makespan markers
    for res, color, name in ((imm, C_IMM, "Immediate"),
                             (dfr, C_DEF, "Deferred")):
        ax_c.axvline(res.makespan, color=color, linestyle="--",
                     linewidth=0.9, alpha=0.7)
        ax_c.annotate(
            f"{name}\n$T = {res.makespan:.0f}$ с\n($C = {res.concurrency}$ цеп.)",
            xy=(res.makespan, n_chains * 0.5),
            xytext=(res.makespan + dfr.makespan * 0.02, n_chains * 0.42),
            color=color, fontsize=9,
            va="center", ha="left",
        )

    ax_c.set_xlim(0, dfr.makespan * 1.18)
    ax_c.set_ylim(0, n_chains * 1.12)
    ax_c.set_xlabel("Время $t$, c")
    ax_c.set_ylabel("Завершено цепочек (нарастающим итогом)")
    ax_c.set_title(
        f"(б) Пропускная способность: Immediate разгребает нагрузку "
        f"в {speedup:.0f} раз быстрее"
    )
    ax_c.spines["top"].set_visible(False)
    ax_c.spines["right"].set_visible(False)
    ax_c.legend(loc="center right", frameon=False)

    fig.suptitle(
        f"Backlog {n_chains} цепочек,  $L = {chain_length}$ задач,  "
        f"$\\tau = {duration:.0f}$ с,  $V_{{\\max}} = {v_max:.0f}$ усл. ед.",
        fontsize=11,
    )
    plt.tight_layout(rect=(0, 0, 1, 0.98))

    OUT_DIR.mkdir(exist_ok=True)
    pdf = OUT_DIR / "policies_throughput.pdf"
    png = OUT_DIR / "policies_throughput.png"
    fig.savefig(pdf)
    fig.savefig(png, dpi=200)
    print(f"saved {pdf.relative_to(Path.cwd())}")
    print(f"saved {png.relative_to(Path.cwd())}")
    print(f"Immediate: C={imm.concurrency}, makespan={imm.makespan:.0f}s")
    print(f"Deferred:  C={dfr.concurrency}, makespan={dfr.makespan:.0f}s")
    print(f"speedup = {speedup:.1f}x")


if __name__ == "__main__":
    run()
