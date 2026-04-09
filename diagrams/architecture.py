"""Generate the Data Platform Architecture diagram as a PDF."""
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

OUTPUT = Path(__file__).parent / "architecture.pdf"

C_SOURCES   = "#E8F4FD"
C_ORCH      = "#FFF3CD"
C_STORAGE   = "#D4EDDA"
C_PROCESS   = "#FCE4EC"
C_GOV       = "#EDE7F6"
C_CONSUME   = "#FFF8E1"
C_BORDER    = "#455A64"
C_ARROW     = "#37474F"
C_MEDALLION = "#B3E5FC"
FONT        = "DejaVu Sans"

BX = 1.5   # left edge of all boxes
BW = 7.0   # width of all boxes


def _box(
    ax: plt.Axes,
    x: float, y_bottom: float, w: float, h: float,
    label: str, sublabel: str,
    facecolor: str,
) -> None:
    ax.add_patch(FancyBboxPatch(
        (x, y_bottom), w, h,
        boxstyle="round,pad=0.02",
        facecolor=facecolor, edgecolor=C_BORDER,
        linewidth=1.5, zorder=2,
    ))
    label_y = y_bottom + h / 2 + (0.08 if sublabel else 0)
    ax.text(x + w / 2, label_y, label,
            ha="center", va="center", fontsize=11,
            fontweight="bold", fontfamily=FONT, color="#212121", zorder=3)
    if sublabel:
        ax.text(x + w / 2, y_bottom + h / 2 - 0.12, sublabel,
                ha="center", va="center", fontsize=8.5,
                fontfamily=FONT, color="#546E7A", style="italic", zorder=3)


def _arrow(ax: plt.Axes, y_from: float, y_to: float) -> None:
    ax.annotate(
        "", xy=(BX + BW / 2, y_to), xytext=(BX + BW / 2, y_from),
        arrowprops=dict(arrowstyle="-|>", color=C_ARROW, lw=1.8, mutation_scale=18),
        zorder=1,
    )


def main() -> None:
    fig, ax = plt.subplots(figsize=(10, 15))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 15)
    ax.axis("off")
    fig.patch.set_facecolor("#FAFAFA")

    fig.suptitle(
        "IOC Analytics Platform — Data Architecture",
        fontsize=15, fontweight="bold", fontfamily=FONT, color="#212121", y=0.93,
    )

    # ── Layer positions (y_bottom, height) ────────────────────────────────────
    Y_SOURCES = 12.8
    Y_CRON    = 11.4
    Y_STORAGE = 9.3
    Y_SPARK   = 7.8
    Y_GOV     = 6.4
    Y_DUCKDB  = 5.0

    H_STD     = 1.0
    H_STORAGE = 1.8

    # ── Draw boxes ────────────────────────────────────────────────────────────
    _box(ax, BX, Y_SOURCES, BW, H_STD,
         "Data Sources",
         "CSV files  ·  REST APIs  ·  Operational databases", C_SOURCES)

    _box(ax, BX, Y_CRON, BW, H_STD,
         "Cron Job",
         "Triggers the pipeline once per Olympic edition", C_ORCH)

    _box(ax, BX, Y_STORAGE, BW, H_STORAGE,
         "MinIO  +  Delta Lake",
         "Object storage (data lake)  ·  ACID transactions  ·  time-travel", C_STORAGE)

    _box(ax, BX, Y_SPARK, BW, H_STD,
         "Apache Spark",
         "Distributed batch processing  ·  ML / Deep Learning", C_PROCESS)

    _box(ax, BX, Y_GOV, BW, H_STD,
         "Governance Layer",
         "Data quality checks  ·  Pipeline lineage tracking", C_GOV)

    _box(ax, BX, Y_DUCKDB, BW, H_STD,
         "DuckDB",
         "Gold layer star schema  ·  Embedded analytical database  ·  Built-in web UI", C_CONSUME)

    # ── Medallion badges inside Storage box ───────────────────────────────────
    badge_h = 0.5
    badge_w = 1.8
    badge_y = Y_STORAGE + 0.15
    for i, (stage, desc) in enumerate([
        ("Bronze", "raw"),
        ("Silver", "clean"),
        ("Gold",   "star schema"),
    ]):
        bx = BX + 0.35 + i * 2.15
        ax.add_patch(FancyBboxPatch(
            (bx, badge_y), badge_w, badge_h,
            boxstyle="round,pad=0.01",
            facecolor=C_MEDALLION, edgecolor="#0288D1",
            linewidth=1.0, zorder=4,
        ))
        ax.text(bx + badge_w / 2, badge_y + 0.33, stage,
                ha="center", va="center", fontsize=8.5,
                fontweight="bold", color="#01579B", zorder=5)
        ax.text(bx + badge_w / 2, badge_y + 0.13, f"({desc})",
                ha="center", va="center", fontsize=7.5,
                color="#0277BD", zorder=5)
        if i < 2:
            ax.annotate("", xy=(bx + badge_w + 0.32, badge_y + badge_h / 2),
                        xytext=(bx + badge_w + 0.02, badge_y + badge_h / 2),
                        arrowprops=dict(arrowstyle="-|>", color="#0288D1", lw=1.2),
                        zorder=5)

    # ── Arrows between layers ─────────────────────────────────────────────────
    _arrow(ax, Y_SOURCES,            Y_CRON + H_STD)
    _arrow(ax, Y_CRON,               Y_STORAGE + H_STORAGE)
    _arrow(ax, Y_STORAGE,            Y_SPARK + H_STD)
    _arrow(ax, Y_SPARK,              Y_GOV + H_STD)
    _arrow(ax, Y_GOV,                Y_DUCKDB + H_STD)

    # ── Legend ────────────────────────────────────────────────────────────────
    legend_items = [
        (C_SOURCES, "Data Sources"),
        (C_ORCH,    "Orchestration"),
        (C_STORAGE, "Storage"),
        (C_PROCESS, "Processing"),
        (C_GOV,     "Governance"),
        (C_CONSUME, "Consumption"),
    ]
    for i, (color, label) in enumerate(legend_items):
        lx = 1.5 + i * 1.18
        ax.add_patch(FancyBboxPatch((lx, 3.8), 0.28, 0.28,
                                    boxstyle="round,pad=0.01",
                                    facecolor=color, edgecolor=C_BORDER,
                                    linewidth=1.0, zorder=3))
        ax.text(lx + 0.35, 3.94, label, fontsize=7.5,
                va="center", fontfamily=FONT, color="#37474F")

    plt.tight_layout(rect=[0, 0.02, 1, 0.96])
    fig.savefig(OUTPUT, format="pdf", bbox_inches="tight", dpi=150)
    print("Saved: diagrams/architecture.pdf")


if __name__ == "__main__":
    main()
