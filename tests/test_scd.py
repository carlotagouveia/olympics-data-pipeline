"""Unit tests for SCD Type 1 and Type 2 logic."""
from datetime import date

import pandas as pd
import pytest

from src.pipeline.scd import apply_scd_type1, apply_scd_type2

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

LOAD_DATE_2000 = date(2000, 1, 1)
LOAD_DATE_2004 = date(2004, 1, 1)
LOAD_DATE_2008 = date(2008, 1, 1)


def _make_athletes(rows: list[dict]) -> pd.DataFrame:  # type: ignore[type-arg]
    """Helper to build an incoming athletes DataFrame."""
    return pd.DataFrame(rows)


def _make_existing(rows: list[dict]) -> pd.DataFrame:  # type: ignore[type-arg]
    """Helper to build an existing dim_athlete DataFrame (with SCD metadata)."""
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# SCD Type 2 tests
# ---------------------------------------------------------------------------

class TestSCDType2:

    def test_first_load_assigns_surrogate_keys(self) -> None:
        """On first load every record gets a sequential SK starting at 1."""
        incoming = _make_athletes([
            {"athlete_nk": 1, "name": "Alice", "team": "USA"},
            {"athlete_nk": 2, "name": "Bob",   "team": "GBR"},
        ])

        result = apply_scd_type2(
            existing=pd.DataFrame(),
            incoming=incoming,
            natural_key="athlete_nk",
            tracked_cols=["name", "team"],
            surrogate_key="athlete_sk",
            load_date=LOAD_DATE_2000,
        )

        assert list(result["athlete_sk"]) == [1, 2]
        assert all(result["is_current"])
        assert all(result["valid_from"] == pd.Timestamp(LOAD_DATE_2000))
        assert all(result["valid_to"] == pd.Timestamp("9999-12-31"))

    def test_no_changes_produces_no_new_versions(self) -> None:
        """When incoming data is identical to existing, no new rows are created."""
        existing = _make_existing([
            {"athlete_sk": 1, "athlete_nk": 1, "name": "Alice", "team": "USA",
             "valid_from": pd.Timestamp("2000-01-01"), "valid_to": pd.Timestamp("9999-12-31"),
             "is_current": True},
        ])
        incoming = _make_athletes([{"athlete_nk": 1, "name": "Alice", "team": "USA"}])

        result = apply_scd_type2(
            existing=existing,
            incoming=incoming,
            natural_key="athlete_nk",
            tracked_cols=["name", "team"],
            surrogate_key="athlete_sk",
            load_date=LOAD_DATE_2004,
        )

        assert len(result) == 1
        assert result.iloc[0]["is_current"] == True

    def test_changed_record_expires_old_and_creates_new_version(self) -> None:
        """When a tracked column changes, the old row is expired and a new one inserted."""
        existing = _make_existing([
            {"athlete_sk": 1, "athlete_nk": 2, "name": "Bob Jones", "team": "GBR",
             "valid_from": pd.Timestamp("2000-01-01"), "valid_to": pd.Timestamp("9999-12-31"),
             "is_current": True},
        ])
        incoming = _make_athletes([{"athlete_nk": 2, "name": "Bob Johnson", "team": "GBR"}])

        result = apply_scd_type2(
            existing=existing,
            incoming=incoming,
            natural_key="athlete_nk",
            tracked_cols=["name", "team"],
            surrogate_key="athlete_sk",
            load_date=LOAD_DATE_2004,
        )

        assert len(result) == 2

        old = result[result["athlete_sk"] == 1].iloc[0]
        assert old["is_current"] == False
        assert old["valid_to"] == pd.Timestamp(LOAD_DATE_2004)
        assert old["name"] == "Bob Jones"

        new = result[result["is_current"] == True].iloc[0]
        assert new["name"] == "Bob Johnson"
        assert new["valid_from"] == pd.Timestamp(LOAD_DATE_2004)
        assert new["valid_to"] == pd.Timestamp("9999-12-31")

    def test_new_record_is_inserted(self) -> None:
        """A natural key never seen before is inserted as a brand-new current record."""
        existing = _make_existing([
            {"athlete_sk": 1, "athlete_nk": 1, "name": "Alice", "team": "USA",
             "valid_from": pd.Timestamp("2000-01-01"), "valid_to": pd.Timestamp("9999-12-31"),
             "is_current": True},
        ])
        incoming = _make_athletes([
            {"athlete_nk": 1, "name": "Alice", "team": "USA"},  # unchanged
            {"athlete_nk": 3, "name": "Carol", "team": "CHN"},  # brand new
        ])

        result = apply_scd_type2(
            existing=existing,
            incoming=incoming,
            natural_key="athlete_nk",
            tracked_cols=["name", "team"],
            surrogate_key="athlete_sk",
            load_date=LOAD_DATE_2004,
        )

        assert len(result) == 2
        carol = result[result["athlete_nk"] == 3].iloc[0]
        assert carol["is_current"] == True
        assert carol["name"] == "Carol"

    def test_surrogate_keys_are_unique(self) -> None:
        """Every row in the result must have a unique surrogate key."""
        existing = _make_existing([
            {"athlete_sk": 1, "athlete_nk": 1, "name": "Alice", "team": "USA",
             "valid_from": pd.Timestamp("2000-01-01"), "valid_to": pd.Timestamp("9999-12-31"),
             "is_current": True},
            {"athlete_sk": 2, "athlete_nk": 2, "name": "Bob Jones", "team": "GBR",
             "valid_from": pd.Timestamp("2000-01-01"), "valid_to": pd.Timestamp("9999-12-31"),
             "is_current": True},
        ])
        incoming = _make_athletes([
            {"athlete_nk": 2, "name": "Bob Johnson", "team": "GBR"},  # changed
            {"athlete_nk": 3, "name": "Carol", "team": "CHN"},         # new
        ])

        result = apply_scd_type2(
            existing=existing,
            incoming=incoming,
            natural_key="athlete_nk",
            tracked_cols=["name", "team"],
            surrogate_key="athlete_sk",
            load_date=LOAD_DATE_2004,
        )

        assert result["athlete_sk"].nunique() == len(result)

    def test_multiple_batches_build_correct_history(self) -> None:
        """Processing three batches in order produces the correct historical chain."""
        # Batch 1: Bob Jones
        after_batch1 = apply_scd_type2(
            existing=pd.DataFrame(),
            incoming=_make_athletes([{"athlete_nk": 2, "name": "Bob Jones", "team": "GBR"}]),
            natural_key="athlete_nk",
            tracked_cols=["name", "team"],
            surrogate_key="athlete_sk",
            load_date=LOAD_DATE_2000,
        )

        # Batch 2: Bob changes name
        after_batch2 = apply_scd_type2(
            existing=after_batch1,
            incoming=_make_athletes([{"athlete_nk": 2, "name": "Bob Johnson", "team": "GBR"}]),
            natural_key="athlete_nk",
            tracked_cols=["name", "team"],
            surrogate_key="athlete_sk",
            load_date=LOAD_DATE_2004,
        )

        # Batch 3: Bob changes team
        after_batch3 = apply_scd_type2(
            existing=after_batch2,
            incoming=_make_athletes([{"athlete_nk": 2, "name": "Bob Johnson", "team": "USA"}]),
            natural_key="athlete_nk",
            tracked_cols=["name", "team"],
            surrogate_key="athlete_sk",
            load_date=LOAD_DATE_2008,
        )

        history = after_batch3[after_batch3["athlete_nk"] == 2].sort_values("valid_from")

        assert len(history) == 3
        assert history.iloc[0]["name"] == "Bob Jones"
        assert history.iloc[0]["is_current"] == False
        assert history.iloc[1]["name"] == "Bob Johnson"
        assert history.iloc[1]["is_current"] == False
        assert history.iloc[2]["team"] == "USA"
        assert history.iloc[2]["is_current"] == True


# ---------------------------------------------------------------------------
# SCD Type 1 tests
# ---------------------------------------------------------------------------

class TestSCDType1:

    def test_first_load_returns_incoming(self) -> None:
        """On first load the result equals the incoming DataFrame."""
        incoming = _make_athletes([
            {"event_name": "100m Men", "sport": "Athletics", "season": "Summer"},
        ])

        result = apply_scd_type1(
            existing=pd.DataFrame(),
            incoming=incoming,
            natural_key="event_name",
            tracked_cols=["sport", "season"],
        )

        assert len(result) == 1
        assert result.iloc[0]["sport"] == "Athletics"

    def test_changed_value_is_overwritten(self) -> None:
        """When a tracked column changes, the existing row is updated in-place."""
        existing = pd.DataFrame([
            {"event_name": "100m Men", "sport": "Athletics", "season": "Summer"},
        ])
        incoming = pd.DataFrame([
            {"event_name": "100m Men", "sport": "Track and Field", "season": "Summer"},
        ])

        result = apply_scd_type1(
            existing=existing,
            incoming=incoming,
            natural_key="event_name",
            tracked_cols=["sport", "season"],
        )

        assert len(result) == 1
        assert result.iloc[0]["sport"] == "Track and Field"

    def test_new_record_is_appended(self) -> None:
        """A new natural key is appended to the existing records."""
        existing = pd.DataFrame([
            {"event_name": "100m Men", "sport": "Athletics", "season": "Summer"},
        ])
        incoming = pd.DataFrame([
            {"event_name": "200m Men", "sport": "Athletics", "season": "Summer"},
        ])

        result = apply_scd_type1(
            existing=existing,
            incoming=incoming,
            natural_key="event_name",
            tracked_cols=["sport", "season"],
        )

        assert len(result) == 2
        assert set(result["event_name"]) == {"100m Men", "200m Men"}

    def test_unchanged_record_is_not_duplicated(self) -> None:
        """An unchanged record stays as a single row."""
        existing = pd.DataFrame([
            {"event_name": "100m Men", "sport": "Athletics", "season": "Summer"},
        ])
        incoming = pd.DataFrame([
            {"event_name": "100m Men", "sport": "Athletics", "season": "Summer"},
        ])

        result = apply_scd_type1(
            existing=existing,
            incoming=incoming,
            natural_key="event_name",
            tracked_cols=["sport", "season"],
        )

        assert len(result) == 1
