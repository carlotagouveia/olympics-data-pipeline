"""Unit tests for the governance layer (quality checks and lineage)."""
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import duckdb

from src.governance.quality import (
    check_not_empty,
    check_no_nulls,
    check_unique,
    check_accepted_values,
    check_row_count_consistent,
)
from src.governance.lineage import init_lineage, log_lineage


# Quality checks 

class TestCheckNotEmpty:

    def test_passes_for_non_empty_dataframe(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = check_not_empty(df, "test_table")
        assert result.passed == True

    def test_fails_for_empty_dataframe(self) -> None:
        df = pd.DataFrame({"a": []})
        result = check_not_empty(df, "test_table")
        assert result.passed == False


class TestCheckNoNulls:

    def test_passes_when_no_nulls(self) -> None:
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        result = check_no_nulls(df, ["id", "name"], "test_table")
        assert result.passed == True

    def test_fails_when_null_present(self) -> None:
        df = pd.DataFrame({"id": [1, None], "name": ["Alice", "Bob"]})
        result = check_no_nulls(df, ["id"], "test_table")
        assert result.passed == False

    def test_ignores_unlisted_columns(self) -> None:
        df = pd.DataFrame({"id": [1, 2], "optional": [None, None]})
        result = check_no_nulls(df, ["id"], "test_table")
        assert result.passed == True


class TestCheckUnique:

    def test_passes_when_all_unique(self) -> None:
        df = pd.DataFrame({"noc": ["USA", "GBR", "CHN"]})
        result = check_unique(df, ["noc"], "test_table")
        assert result.passed == True

    def test_fails_when_duplicates_exist(self) -> None:
        df = pd.DataFrame({"noc": ["USA", "USA", "CHN"]})
        result = check_unique(df, ["noc"], "test_table")
        assert result.passed == False


class TestCheckAcceptedValues:

    def test_passes_when_all_values_accepted(self) -> None:
        df = pd.DataFrame({"season": ["Summer", "Winter", "Summer"]})
        result = check_accepted_values(df, "season", {"Summer", "Winter"}, "test_table")
        assert result.passed == True

    def test_fails_when_unexpected_value_present(self) -> None:
        df = pd.DataFrame({"season": ["Summer", "Spring"]})
        result = check_accepted_values(df, "season", {"Summer", "Winter"}, "test_table")
        assert result.passed == False

    def test_ignores_nulls(self) -> None:
        df = pd.DataFrame({"medal": ["Gold", None, "Bronze"]})
        result = check_accepted_values(df, "medal", {"Gold", "Silver", "Bronze", None}, "test_table")
        assert result.passed == True


class TestCheckRowCountConsistent:

    def test_passes_when_counts_match(self) -> None:
        result = check_row_count_consistent(100, 100, "test_table")
        assert result.passed == True

    def test_fails_when_rows_dropped(self) -> None:
        result = check_row_count_consistent(100, 50, "test_table")
        assert result.passed == False

    def test_passes_within_tolerance(self) -> None:
        result = check_row_count_consistent(100, 95, "test_table", tolerance=0.10)
        assert result.passed == True


# Lineage

class TestLineage:

    @pytest.fixture()
    def conn(self) -> duckdb.DuckDBPyConnection:
        c = duckdb.connect(":memory:")
        init_lineage(c)
        return c

    def test_init_creates_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        tables = conn.execute("SHOW TABLES").df()
        assert "pipeline_lineage" in tables["name"].values

    def test_log_lineage_inserts_record(self, conn: duckdb.DuckDBPyConnection) -> None:
        log_lineage(conn, "bronze", "raw/events.csv", "bronze/events", 1000)
        df = conn.execute("SELECT * FROM pipeline_lineage").df()
        assert len(df) == 1
        assert df.iloc[0]["layer"] == "bronze"
        assert df.iloc[0]["row_count"] == 1000
        assert df.iloc[0]["status"] == "success"

    def test_multiple_records_have_unique_ids(self, conn: duckdb.DuckDBPyConnection) -> None:
        log_lineage(conn, "bronze", "src1", "tgt1", 100)
        log_lineage(conn, "silver", "src2", "tgt2", 90)
        df = conn.execute("SELECT * FROM pipeline_lineage").df()
        assert len(df) == 2
        assert df["lineage_id"].nunique() == 2
