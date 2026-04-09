from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from src.models.schemas import ATHLETE_TRACKED_COLS, NOC_TRACKED_COLS, EVENT_TRACKED_COLS
from src.governance.lineage import init_lineage, log_lineage
from src.pipeline.scd import apply_scd_type1, apply_scd_type2
from src.utils.logger import get_logger

logger = get_logger(__name__)



# Schema initialisation

_DDL = """
CREATE TABLE IF NOT EXISTS dim_game (
    game_sk   BIGINT PRIMARY KEY,
    games     VARCHAR NOT NULL,
    year      INTEGER NOT NULL,
    season    VARCHAR NOT NULL,
    city      VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_event (
    event_sk    BIGINT PRIMARY KEY,
    event_name  VARCHAR NOT NULL,
    sport       VARCHAR NOT NULL,
    season      VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_athlete (
    athlete_sk  BIGINT PRIMARY KEY,
    athlete_nk  BIGINT NOT NULL,
    name        VARCHAR,
    sex         VARCHAR,
    team        VARCHAR,
    valid_from  DATE NOT NULL,
    valid_to    DATE NOT NULL,
    is_current  BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_noc (
    noc_sk      BIGINT PRIMARY KEY,
    noc_code    VARCHAR NOT NULL,
    region      VARCHAR,
    notes       VARCHAR,
    valid_from  DATE NOT NULL,
    valid_to    DATE NOT NULL,
    is_current  BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_results (
    result_sk   BIGINT PRIMARY KEY,
    athlete_sk  BIGINT,
    event_sk    BIGINT,
    noc_sk      BIGINT,
    game_sk     BIGINT,
    medal       VARCHAR,
    age         DOUBLE,
    height      DOUBLE,
    weight      DOUBLE
);
"""


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    for statement in _DDL.strip().split(";"):
        stmt = statement.strip()
        if stmt:
            conn.execute(stmt)


def _table_df(conn: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """Return the full contents of *table* as a DataFrame, or empty if absent."""
    try:
        return conn.execute(f"SELECT * FROM {table}").df()
    except Exception:
        return pd.DataFrame()


# Dimension loaders

def _load_dim_game(
    events: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """SCD Type 0 – insert only; never update existing game records.

    Parameters
    ----------
    events:
        Silver-layer events DataFrame for the current batch.
    conn:
        Open DuckDB connection.
    """
    incoming: pd.DataFrame = (
        events[["games", "year", "season", "city"]]
        .drop_duplicates(subset=["games"])
        .copy()
    )
    # Ensure correct dtypes before inserting
    incoming["year"] = incoming["year"].astype(int)

    existing: pd.DataFrame = _table_df(conn, "dim_game")

    if existing.empty:
        new_games = incoming.copy()
    else:
        known: set[object] = set(existing["games"])
        new_games = incoming[~incoming["games"].isin(known)].copy()

    if new_games.empty:
        return

    max_sk: int = int(existing["game_sk"].max()) if not existing.empty else 0
    new_games["game_sk"] = range(max_sk + 1, max_sk + 1 + len(new_games))

    conn.register("_new_games", new_games)
    conn.execute(
        "INSERT INTO dim_game SELECT game_sk, games, year, season, city FROM _new_games"
    )
    logger.info("dim_game: inserted %s new records", len(new_games))


def _load_dim_event(
    events: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """SCD Type 1 – overwrite metadata; no historical versions kept.

    Parameters
    ----------
    events:
        Silver-layer events DataFrame for the current batch.
    conn:
        Open DuckDB connection.
    """
    incoming: pd.DataFrame = (
        events[["event", "sport", "season"]]
        .drop_duplicates(subset=["event"])
        .rename(columns={"event": "event_name"})
        .copy()
    )

    existing: pd.DataFrame = _table_df(conn, "dim_event")

    updated: pd.DataFrame = apply_scd_type1(
        existing=existing,
        incoming=incoming,
        natural_key="event_name",
        tracked_cols=EVENT_TRACKED_COLS,
    )

    # Assign surrogate keys to any row that doesn't have one yet
    if "event_sk" not in updated.columns:
        updated["event_sk"] = range(1, len(updated) + 1)
    else:
        missing_mask: pd.Series = updated["event_sk"].isna()  # type: ignore[type-arg]
        if missing_mask.any():
            max_sk: int = int(updated["event_sk"].dropna().max())
            new_count: int = int(missing_mask.sum())
            updated.loc[missing_mask, "event_sk"] = range(
                max_sk + 1, max_sk + 1 + new_count
            )
    updated["event_sk"] = updated["event_sk"].astype(int)

    conn.execute("DELETE FROM dim_event")
    conn.register("_updated_events", updated)
    conn.execute(
        "INSERT INTO dim_event SELECT event_sk, event_name, sport, season FROM _updated_events"
    )
    logger.info("dim_event: %s records (SCD1)", len(updated))


def _load_dim_athlete(
    events: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    load_date: date,
) -> None:
    """SCD Type 2 – new version created when name, sex, or team changes.

    Parameters
    ----------
    events:
        Silver-layer events DataFrame for the current batch.
    conn:
        Open DuckDB connection.
    load_date:
        Effective date of this batch; used as ``valid_from`` for new rows.
    """
    incoming: pd.DataFrame = (
        events[["athlete_id", "name", "sex", "team"]]
        .drop_duplicates(subset=["athlete_id"])
        .rename(columns={"athlete_id": "athlete_nk"})
        .copy()
    )
    incoming["athlete_nk"] = incoming["athlete_nk"].astype(int)

    existing: pd.DataFrame = _table_df(conn, "dim_athlete")
    if not existing.empty:
        existing["athlete_nk"] = existing["athlete_nk"].astype(int)

    updated: pd.DataFrame = apply_scd_type2(
        existing=existing,
        incoming=incoming,
        natural_key="athlete_nk",
        tracked_cols=ATHLETE_TRACKED_COLS,
        surrogate_key="athlete_sk",
        load_date=load_date,
    )
    updated["athlete_sk"] = updated["athlete_sk"].astype(int)

    conn.execute("DELETE FROM dim_athlete")
    conn.register("_updated_athletes", updated)
    conn.execute(
        """
        INSERT INTO dim_athlete
        SELECT athlete_sk, athlete_nk, name, sex, team,
               valid_from::DATE, valid_to::DATE, is_current
        FROM _updated_athletes
        """
    )
    logger.info("dim_athlete: %s records (SCD2)", len(updated))


def _load_dim_noc(
    noc: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    load_date: date,
) -> None:
    """SCD Type 2 – new version created when region or notes changes.

    Parameters
    ----------
    noc:
        Silver-layer NOC regions DataFrame.
    conn:
        Open DuckDB connection.
    load_date:
        Effective date of this batch; used as ``valid_from`` for new rows.
    """
    incoming: pd.DataFrame = noc.rename(columns={"noc": "noc_code"}).copy()

    existing: pd.DataFrame = _table_df(conn, "dim_noc")

    updated: pd.DataFrame = apply_scd_type2(
        existing=existing,
        incoming=incoming,
        natural_key="noc_code",
        tracked_cols=NOC_TRACKED_COLS,
        surrogate_key="noc_sk",
        load_date=load_date,
    )
    updated["noc_sk"] = updated["noc_sk"].astype(int)

    conn.execute("DELETE FROM dim_noc")
    conn.register("_updated_noc", updated)
    conn.execute(
        """
        INSERT INTO dim_noc
        SELECT noc_sk, noc_code, region, notes,
               valid_from::DATE, valid_to::DATE, is_current
        FROM _updated_noc
        """
    )
    logger.info("dim_noc: %s records (SCD2)", len(updated))



# Fact table loader

def _load_fact_results(
    events: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Append new fact rows for the current batch.

    Parameters
    ----------
    events:
        Silver-layer events DataFrame for the current batch.
    conn:
        Open DuckDB connection (dimensions must already be loaded).
    """
    # Resolve surrogate keys from dimension tables
    athletes: pd.DataFrame = conn.execute(
        "SELECT athlete_sk, athlete_nk FROM dim_athlete WHERE is_current"
    ).df()
    dim_events: pd.DataFrame = conn.execute(
        "SELECT event_sk, event_name FROM dim_event"
    ).df()
    nocs: pd.DataFrame = conn.execute(
        "SELECT noc_sk, noc_code FROM dim_noc WHERE is_current"
    ).df()
    games: pd.DataFrame = conn.execute(
        "SELECT game_sk, games FROM dim_game"
    ).df()

    facts: pd.DataFrame = events.copy()
    facts = facts.rename(columns={"event": "event_name", "athlete_id": "athlete_nk"})
    facts["athlete_nk"] = facts["athlete_nk"].astype(int)
    athletes["athlete_nk"] = athletes["athlete_nk"].astype(int)

    facts = facts.merge(athletes, on="athlete_nk", how="left")
    facts = facts.merge(dim_events, on="event_name", how="left")
    facts = facts.merge(nocs.rename(columns={"noc_code": "noc"}), on="noc", how="left")
    facts = facts.merge(games, on="games", how="left")

    # Determine the max existing result_sk to continue the sequence
    max_sk_result = conn.execute("SELECT COALESCE(MAX(result_sk), 0) FROM fact_results").fetchone()
    max_sk: int = int(max_sk_result[0]) if max_sk_result else 0

    fact_cols: list[str] = ["athlete_sk", "event_sk", "noc_sk", "game_sk", "medal", "age", "height", "weight"]
    facts_clean: pd.DataFrame = facts[fact_cols].copy()
    facts_clean["result_sk"] = range(max_sk + 1, max_sk + 1 + len(facts_clean))

    conn.register("_new_facts", facts_clean)
    conn.execute(
        """
        INSERT INTO fact_results
        SELECT result_sk, athlete_sk, event_sk, noc_sk, game_sk,
               medal, age, height, weight
        FROM _new_facts
        """
    )
    logger.info("fact_results: inserted %s rows", f"{len(facts_clean):,}")



# Public API

def load_batch(
    events_path: Path,
    noc_path: Path,
    db_path: Path,
    load_date: date,
) -> None:
    """Load one silver-layer batch into the gold-layer star schema.

    Parameters
    ----------
    events_path:
        Silver Parquet for athlete-events (may be a full or partial slice).
    noc_path:
        Silver Parquet for NOC regions.
    db_path:
        DuckDB database file (created if it does not exist).
    load_date:
        Effective date of this batch; used as ``valid_from`` in SCD2 tables.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db_path))

    try:
        _init_schema(conn)
        init_lineage(conn)

        events: pd.DataFrame = pd.read_parquet(events_path)
        noc: pd.DataFrame = pd.read_parquet(noc_path)

        _load_dim_game(events, conn)
        log_lineage(conn, "gold", events_path.name, "dim_game", len(events))

        _load_dim_event(events, conn)
        log_lineage(conn, "gold", events_path.name, "dim_event", len(events))

        _load_dim_athlete(events, conn, load_date)
        log_lineage(conn, "gold", events_path.name, "dim_athlete", len(events))

        _load_dim_noc(noc, conn, load_date)
        log_lineage(conn, "gold", noc_path.name, "dim_noc", len(noc))

        _load_fact_results(events, conn)
        log_lineage(conn, "gold", events_path.name, "fact_results", len(events))

        logger.info("Gold layer batch complete [load_date=%s]", load_date)
    finally:
        conn.close()
