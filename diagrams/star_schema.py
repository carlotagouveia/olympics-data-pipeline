"""Generate the Star Schema diagram as a PDF."""
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

OUTPUT = Path(__file__).parent / "star_schema.pdf"

FONT      = "DejaVu Sans"
C_FACT    = "#FCE4EC"
C_DIM     = "#E3F2FD"
C_HEADER_FACT = "#E91E63"
C_HEADER_DIM  = "#1565C0"
C_BORDER  = "#455A64"
C_SCD0    = "#E8F5E9"
C_SCD1    = "#FFF9C4"
C_SCD2    = "#E8EAF6"
C_BORDER_SCD0 = "#388E3C"
C_BORDER_SCD1 = "#F9A825"
C_BORDER_SCD2 = "#3949AB"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _table(
    ax: plt.Axes,
    x: float, y: float, w: float,
    title: str, scd_label: str,
    columns: list[tuple[str, str, str]],   # (name, type, scd_type)
    header_color: str,
    face_color: str,
) -> float:
    """Draw a dimension/fact table. Returns the bottom y of the box."""
    row_h = 0.32
    header_h = 0.42
    total_h = header_h + len(columns) * row_h + 0.1

    # outer box
    ax.add_patch(FancyBboxPatch(
        (x, y - total_h), w, total_h,
        boxstyle="round,pad=0.015",
        facecolor=face_color, edgecolor=C_BORDER,
        linewidth=1.8, zorder=2,
    ))

    # header band
    ax.add_patch(FancyBboxPatch(
        (x, y - header_h), w, header_h,
        boxstyle="round,pad=0.01",
        facecolor=header_color, edgecolor="none",
        linewidth=0, zorder=3,
    ))
    ax.text(x + w / 2, y - header_h / 2, title,
            ha="center", va="center", fontsize=10,
            fontweight="bold", color="white", fontfamily=FONT, zorder=4)
    ax.text(x + w - 0.08, y - header_h / 2, scd_label,
            ha="right", va="center", fontsize=7.5,
            color="white", fontfamily=FONT, style="italic", zorder=4)

    # column rows
    for i, (col_name, col_type, scd) in enumerate(columns):
        ry = y - header_h - (i + 0.5) * row_h - 0.05

        # SCD badge colour
        if scd == "SCD 0":
            badge_fc, badge_ec = C_SCD0, C_BORDER_SCD0
        elif scd == "SCD 1":
            badge_fc, badge_ec = C_SCD1, C_BORDER_SCD1
        elif scd == "SCD 2":
            badge_fc, badge_ec = C_SCD2, C_BORDER_SCD2
        else:
            badge_fc = badge_ec = "none"

        # separator line
        if i > 0:
            ax.plot([x + 0.05, x + w - 0.05],
                    [y - header_h - i * row_h - 0.05,
                     y - header_h - i * row_h - 0.05],
                    color="#CFD8DC", lw=0.6, zorder=3)

        # column name
        ax.text(x + 0.12, ry, col_name,
                ha="left", va="center", fontsize=8.2,
                fontfamily=FONT, color="#212121", zorder=4)

        # type
        ax.text(x + w * 0.58, ry, col_type,
                ha="left", va="center", fontsize=7.5,
                fontfamily=FONT, color="#607D8B", zorder=4)

        # SCD badge
        if scd:
            bw, bh = 0.52, 0.20
            bx2 = x + w - bw - 0.08
            by2 = ry - bh / 2
            ax.add_patch(FancyBboxPatch(
                (bx2, by2), bw, bh,
                boxstyle="round,pad=0.01",
                facecolor=badge_fc, edgecolor=badge_ec,
                linewidth=0.8, zorder=5,
            ))
            ax.text(bx2 + bw / 2, ry, scd,
                    ha="center", va="center", fontsize=6.8,
                    fontweight="bold", color=badge_ec,
                    fontfamily=FONT, zorder=6)

    return y - total_h


def _connector(
    ax: plt.Axes,
    x1: float, y1: float,
    x2: float, y2: float,
) -> None:
    ax.annotate(
        "", xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(
            arrowstyle="-|>",
            color="#78909C", lw=1.5,
            connectionstyle="arc3,rad=0.0",
        ),
        zorder=1,
    )


# ── Layout constants ──────────────────────────────────────────────────────────

W_FACT = 3.8
W_DIM  = 3.4

FACT_COLS = [
    ("result_sk (PK)", "BIGINT", ""),
    ("athlete_sk (FK)", "BIGINT", ""),
    ("event_sk (FK)",   "BIGINT", ""),
    ("noc_sk (FK)",     "BIGINT", ""),
    ("game_sk (FK)",    "BIGINT", ""),
    ("medal",           "VARCHAR", ""),
    ("age",             "DOUBLE",  ""),
    ("height",          "DOUBLE",  ""),
    ("weight",          "DOUBLE",  ""),
]

DIM_ATHLETE_COLS = [
    ("athlete_sk (PK)", "BIGINT",  ""),
    ("athlete_nk",      "BIGINT",  "SCD 0"),
    ("name",            "VARCHAR", "SCD 2"),
    ("sex",             "VARCHAR", "SCD 2"),
    ("team",            "VARCHAR", "SCD 2"),
    ("valid_from",      "DATE",    ""),
    ("valid_to",        "DATE",    ""),
    ("is_current",      "BOOLEAN", ""),
]

DIM_EVENT_COLS = [
    ("event_sk (PK)", "BIGINT",  ""),
    ("event_name",    "VARCHAR", "SCD 0"),
    ("sport",         "VARCHAR", "SCD 1"),
    ("season",        "VARCHAR", "SCD 1"),
]

DIM_NOC_COLS = [
    ("noc_sk (PK)", "BIGINT",  ""),
    ("noc_code",    "VARCHAR", "SCD 0"),
    ("region",      "VARCHAR", "SCD 2"),
    ("notes",       "VARCHAR", "SCD 2"),
    ("valid_from",  "DATE",    ""),
    ("valid_to",    "DATE",    ""),
    ("is_current",  "BOOLEAN", ""),
]

DIM_GAME_COLS = [
    ("game_sk (PK)", "BIGINT",  ""),
    ("games",        "VARCHAR", "SCD 0"),
    ("year",         "INTEGER", "SCD 0"),
    ("season",       "VARCHAR", "SCD 0"),
    ("city",         "VARCHAR", "SCD 0"),
]


def main() -> None:
    fig, ax = plt.subplots(figsize=(14, 13))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 13)
    ax.axis("off")
    fig.patch.set_facecolor("#FAFAFA")

    fig.suptitle(
        "IOC Analytics Platform — Star Schema",
        fontsize=15, fontweight="bold", fontfamily=FONT,
        color="#212121", y=0.93,
    )

    # ── Fact table (centre) ───────────────────────────────────────────────────
    FACT_X, FACT_TOP = 5.1, 11.6
    fact_bottom = _table(ax, FACT_X, FACT_TOP, W_FACT,
                         "fact_results", "",
                         FACT_COLS, C_HEADER_FACT, C_FACT)
    fact_cx = FACT_X + W_FACT / 2
    fact_mid = (FACT_TOP + fact_bottom) / 2

    # ── dim_athlete (left) ────────────────────────────────────────────────────
    ATH_X, ATH_TOP = 0.4, 11.2
    ath_bottom = _table(ax, ATH_X, ATH_TOP, W_DIM,
                        "dim_athlete", "SCD Type 2",
                        DIM_ATHLETE_COLS, C_HEADER_DIM, C_DIM)
    ath_mid = (ATH_TOP + ath_bottom) / 2

    # ── dim_event (right) ─────────────────────────────────────────────────────
    EVT_X, EVT_TOP = 10.2, 11.2
    evt_bottom = _table(ax, EVT_X, EVT_TOP, W_DIM,
                        "dim_event", "SCD Type 1",
                        DIM_EVENT_COLS, C_HEADER_DIM, C_DIM)
    evt_mid = (EVT_TOP + evt_bottom) / 2

    # ── dim_noc (bottom-left) ─────────────────────────────────────────────────
    NOC_X, NOC_TOP = 0.4, 6.2
    noc_bottom = _table(ax, NOC_X, NOC_TOP, W_DIM,
                        "dim_noc", "SCD Type 2",
                        DIM_NOC_COLS, C_HEADER_DIM, C_DIM)
    noc_mid = (NOC_TOP + noc_bottom) / 2

    # ── dim_game (bottom-right) ───────────────────────────────────────────────
    GAME_X, GAME_TOP = 10.2, 6.8
    game_bottom = _table(ax, GAME_X, GAME_TOP, W_DIM,
                         "dim_game", "SCD Type 0",
                         DIM_GAME_COLS, C_HEADER_DIM, C_DIM)
    game_mid = (GAME_TOP + game_bottom) / 2

    # ── Connectors ────────────────────────────────────────────────────────────
    # dim_athlete → fact
    _connector(ax, ATH_X + W_DIM, ath_mid, FACT_X, fact_mid)
    # dim_event → fact
    _connector(ax, EVT_X, evt_mid, FACT_X + W_FACT, fact_mid)
    # dim_noc → fact (bottom-left to fact centre-bottom)
    _connector(ax, NOC_X + W_DIM / 2, NOC_TOP,
               fact_cx - 0.5, fact_bottom)
    # dim_game → fact (bottom-right to fact centre-bottom)
    _connector(ax, GAME_X + W_DIM / 2, GAME_TOP,
               fact_cx + 0.5, fact_bottom)

    # ── SCD legend ────────────────────────────────────────────────────────────
    legend_data = [
        (C_SCD0, C_BORDER_SCD0, "SCD Type 0 — Fixed: value never changes"),
        (C_SCD1, C_BORDER_SCD1, "SCD Type 1 — Overwrite: no history kept"),
        (C_SCD2, C_BORDER_SCD2, "SCD Type 2 — New row: full history preserved"),
    ]
    for i, (fc, ec, label) in enumerate(legend_data):
        lx, ly = 0.5, 1.6 - i * 0.45
        ax.add_patch(FancyBboxPatch((lx, ly), 0.55, 0.28,
                                    boxstyle="round,pad=0.01",
                                    facecolor=fc, edgecolor=ec,
                                    linewidth=1.0, zorder=3))
        ax.text(lx + 0.65, ly + 0.14, label,
                fontsize=8.5, va="center", fontfamily=FONT, color="#37474F")

    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    fig.savefig(OUTPUT, format="pdf", bbox_inches="tight", dpi=150)
    print("Saved: diagrams/star_schema.pdf")


if __name__ == "__main__":
    main()
