"""
Branching experiment: how the Immediate-policy advantage M / w depends on
the shape of the processing graph.

For each representative graph we measure, by replaying the graph in
topological order, the two per-graph footprints of the article model:

  - w (cut-width)  — peak simultaneously-live datasets under Immediate;
  - M              — total datasets the graph passes through the buffer
                     (the Deferred footprint).

The Immediate advantage is M / w. It equals the depth L for graphs that do
not multiply data (a linear chain, or many independent parallel chains —
the typical reconstruction workload) and collapses toward 1 as a fan-out
multiplies the data without reconverging.

Usage:
    uv run plot_branching.py        # or: python plot_branching.py

Outputs:
    outputs/branching_advantage.pdf
    outputs/branching_advantage.png
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from topological_gc import build_fanout_tree, measure_footprints

OUT_DIR = Path(__file__).parent / "outputs"
BAR_COLOR = "#1f4e96"


def run() -> None:
    # (label, workflow) — ordered from non-multiplying to heavily branching
    shapes = [
        ("Линейная цепочка\n(глубина 10)", build_fanout_tree(10, 1)),
        ("Дерево\n$b=2$, глубина 5", build_fanout_tree(5, 2)),
        ("Дерево\n$b=3$, глубина 4", build_fanout_tree(4, 3)),
        ("Широкий fan-out\n$b=16$, глубина 2", build_fanout_tree(2, 16)),
    ]

    labels: list[str] = []
    ratios: list[float] = []
    annots: list[str] = []
    for label, wf in shapes:
        w, m = measure_footprints(wf)
        labels.append(label)
        ratios.append(m / w)
        annots.append(f"$M/w={m/w:.1f}$\n($w={w},\\ M={m}$)")

    fig, ax = plt.subplots(figsize=(8.6, 4.6))
    x = range(len(labels))
    bars = ax.bar(x, ratios, color=BAR_COLOR, width=0.6)

    ax.axhline(1.0, color="grey", linestyle=":", linewidth=0.9)
    ax.text(-0.42, 1.0, "нет выигрыша ($M/w=1$) ",
            color="dimgrey", fontsize=8, ha="left", va="bottom")

    for rect, annot in zip(bars, annots):
        ax.text(rect.get_x() + rect.get_width() / 2,
                rect.get_height() + 0.2, annot,
                ha="center", va="bottom", fontsize=9)

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel("Выигрыш событийной политики $M/w$")
    ax.set_ylim(0, max(ratios) * 1.25)
    ax.set_title("Выигрыш Immediate-политики в зависимости от формы графа "
                 "(измерено имитатором)")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    OUT_DIR.mkdir(exist_ok=True)
    pdf = OUT_DIR / "branching_advantage.pdf"
    png = OUT_DIR / "branching_advantage.png"
    fig.savefig(pdf)
    fig.savefig(png, dpi=200)
    print(f"saved {pdf.relative_to(Path.cwd())}")
    print(f"saved {png.relative_to(Path.cwd())}")
    for label, ratio in zip(labels, ratios):
        print(f"  {label.replace(chr(10), ' '):34s} M/w = {ratio:.2f}")


if __name__ == "__main__":
    run()
