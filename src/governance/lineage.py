"""Pipeline lineage tracking — records what was processed, when, and from where."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb

from src.utils.logger import get_logger

logger = get_logger(__name__)

_LINEAGE_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_lineage (
    lineage_id    BIGINT PRIMARY KEY,
    layer         VARCHAR NOT NULL,
    source        VARCHAR NOT NULL,
    target        VARCHAR NOT NULL,
    row_count     BIGINT  NOT NULL,
    status        VARCHAR NOT NULL,
    run_timestamp TIMESTAMP NOT NULL
);
"""


def init_lineage(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the lineage table if it does not exist."""
    conn.execute(_LINEAGE_DDL)


def log_lineage(
    conn: duckdb.DuckDBPyConnection,
    layer: str,
    source: str,
    target: str,
    row_count: int,
    status: str = "success",
) -> None:
    """Insert a lineage record for a pipeline step.

    Parameters
    ----------
    conn:
        Open DuckDB connection.
    layer:
        Pipeline layer (bronze, silver, gold).
    source:
        Source file or table name.
    target:
        Target file or table name.
    row_count:
        Number of rows processed.
    status:
        Outcome of the step (success / failure).
    """
    max_row = conn.execute(
        "SELECT COALESCE(MAX(lineage_id), 0) FROM pipeline_lineage"
    ).fetchone()
    next_id: int = int(max_row[0]) + 1 if max_row else 1

    conn.execute(
        """
        INSERT INTO pipeline_lineage
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [next_id, layer, source, target, row_count, status, datetime.utcnow()],
    )
    logger.info(
        "LINEAGE [%s] %s -> %s (%s rows, %s)",
        layer, source, target, f"{row_count:,}", status,
    )
