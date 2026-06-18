"""
Baseline comparison figure: buffer occupancy over time under a workload with
failures, for three reclamation policies — semantic (CanDelete), naive
reference counting, and a fixed TTL.

The point (the article's stated novelty, here measured): reference counting
leaks on failures — the input of a failed consumer is never released — so the
buffer grows without bound and breaches V_max; the semantic policy frees that
data the instant the consumer fails and stays bounded; a fixed TTL stays
bounded but elevated (it holds every dataset for ttl rather than until it is
actually consumed).

Usage:
    python plot_baselines.py

Outputs:
    outputs/baselines.pdf
    outputs/baselines.png
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from baselines import simulate_baselines, _first_breach

OUT_DIR = Path(__file__).parent / "outputs"

C_SEM = "#1f4e96"   # semantic — the article's method (blue)
C_RC = "#b1361b"    # reference counting — leaks (red)
C_TTL = "#6a6a6a"   # fixed TTL — wasteful (grey)


def run() -> None:
    tr = simulate_baselines(
        L=10, tau=1.0, size=1.0, n_chains=180, arr_interval=1.0,
        fail_period=3, fail_stage=5, ttl=3.0, offload=2.0, v_max=40.0, dt=0.5,
    )
    fail_pct = 100.0 * tr.n_failed / tr.n_chains

    fig, ax = plt.subplots(figsize=(9.0, 5.0), constrained_layout=True)

    ax.step(tr.times, tr.occ_refcount, where="post", color=C_RC, linewidth=2.0,
            label="Подсчёт ссылок (по успеху)")
    ax.step(tr.times, tr.occ_ttl, where="post", color=C_TTL, linewidth=2.0,
            label=f"TTL (срок ${tr.ttl:.0f}\\tau$)")
    ax.step(tr.times, tr.occ_semantic, where="post", color=C_SEM, linewidth=2.0,
            label="Семантическая (CanDelete)")

    # hard buffer ceiling
    ax.axhline(tr.v_max, color="black", linestyle="--", linewidth=1.0)
    ax.text(tr.times[-1], tr.v_max, " $V_{\\max}$", color="black",
            fontsize=10, ha="right", va="bottom")

    # mark where reference counting breaches the ceiling
    breach = _first_breach(tr.times, tr.occ_refcount, tr.v_max)
    if breach is not None:
        ax.axvline(breach, color=C_RC, linestyle=":", linewidth=0.9, alpha=0.7)
        ax.annotate(
            "подсчёт ссылок\nпробивает $V_{\\max}$",
            xy=(breach, tr.v_max), xytext=(breach + 6, tr.v_max + 12),
            color=C_RC, fontsize=9,
            arrowprops=dict(arrowstyle="->", color=C_RC, lw=0.8),
        )

    ax.set_xlabel("Время (в единицах времени стадии $\\tau$)")
    ax.set_ylabel("Занятость буфера (число датасетов)")
    ax.set_title(
        f"Занятость буфера под потоком со сбоями (доля сбоев {fail_pct:.0f}%)",
        fontsize=11,
    )
    ax.set_xlim(0, tr.times[-1])
    ax.set_ylim(0, max(tr.occ_refcount) * 1.12)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="upper left", frameon=False)

    OUT_DIR.mkdir(exist_ok=True)
    pdf = OUT_DIR / "baselines.pdf"
    png = OUT_DIR / "baselines.png"
    fig.savefig(pdf)
    fig.savefig(png, dpi=200)
    print(f"saved {pdf.relative_to(Path.cwd())}")
    print(f"saved {png.relative_to(Path.cwd())}")
    print(f"chains={tr.n_chains} failed={tr.n_failed} ({fail_pct:.0f}%)")
    for name, occ in (("semantic", tr.occ_semantic),
                      ("refcount", tr.occ_refcount),
                      ("ttl", tr.occ_ttl)):
        b = _first_breach(tr.times, occ, tr.v_max)
        print(f"  {name:9s} peak={max(occ):.0f} final={occ[-1]:.0f} "
              f"breach={b}")


if __name__ == "__main__":
    run()
