"""Unit tests for the silver layer cleaning functions."""
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.pipeline.silver import clean_athlete_events, clean_noc_regions
from tests.conftest import requires_spark


@pytest.fixture()
def raw_events_parquet(tmp_path: Path) -> Path:
    """Write a minimal raw events Parquet (mimicking bronze output)."""
    df = pd.DataFrame({
        "ID":     [1,      1,      2,      3],
        "Name":   ["Alice Smith", "Alice Smith", "Bob Jones", "Carol Lin"],
        "Sex":    ["F",    "F",    "M",    "F"],
        "Age":    ["22",   "22",   "NA",   "20"],
        "Height": ["165",  "165",  "180",  "NA"],
        "Weight": ["60",   "60",   "80",   "52"],
        "Team":   [" USA ", "USA",  "GBR",  "CHN"],
        "NOC":    ["USA",  "USA",  "GBR",  "CHN"],
        "Games":  ["2000 Summer", "2000 Summer", "2000 Summer", "2004 Summer"],
        "Year":   [2000,   2000,   2000,   2004],
        "Season": ["Summer", "Summer", "Summer", "Summer"],
        "City":   ["Sydney", "Sydney", "Sydney", "Athens"],
        "Sport":  ["Athletics", "Athletics", "Swimming", "Gymnastics"],
        "Event":  ["100m Women", "100m Women", "200m Men", "Floor Women"],
        "Medal":  ["Gold", "Gold", "NA", "Bronze"],
    })
    path = tmp_path / "events.parquet"
    df.to_parquet(path, index=False)
    return path


@pytest.fixture()
def raw_noc_parquet(tmp_path: Path) -> Path:
    """Write a minimal raw NOC regions Parquet."""
    df = pd.DataFrame({
        "NOC":    ["USA", "GBR", "CHN", "CHN"],
        "region": ["United States", "United Kingdom", "China", "China"],
        "notes":  [None, None, None, None],
    })
    path = tmp_path / "noc.parquet"
    df.to_parquet(path, index=False)
    return path


@requires_spark
class TestCleanAthleteEvents:

    def test_columns_are_renamed_to_snake_case(
        self, spark: SparkSession, raw_events_parquet: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "silver"
        clean_athlete_events(spark, raw_events_parquet, out)
        df = pd.read_parquet(out)
        assert "athlete_id" in df.columns
        assert "ID" not in df.columns

    def test_numeric_columns_are_coerced(
        self, spark: SparkSession, raw_events_parquet: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "silver"
        clean_athlete_events(spark, raw_events_parquet, out)
        df = pd.read_parquet(out)
        bob = df[df["athlete_id"] == 2].iloc[0]
        assert pd.isna(bob["age"])
        carol = df[df["athlete_id"] == 3].iloc[0]
        assert pd.isna(carol["height"])

    def test_medal_na_string_becomes_null(
        self, spark: SparkSession, raw_events_parquet: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "silver"
        clean_athlete_events(spark, raw_events_parquet, out)
        df = pd.read_parquet(out)
        bob = df[df["athlete_id"] == 2].iloc[0]
        assert pd.isna(bob["medal"])

    def test_duplicate_rows_are_removed(
        self, spark: SparkSession, raw_events_parquet: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "silver"
        clean_athlete_events(spark, raw_events_parquet, out)
        df = pd.read_parquet(out)
        assert len(df) == 3

    def test_strings_are_stripped(
        self, spark: SparkSession, raw_events_parquet: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "silver"
        clean_athlete_events(spark, raw_events_parquet, out)
        df = pd.read_parquet(out)
        alice = df[df["athlete_id"] == 1].iloc[0]
        assert alice["team"] == "USA"


@requires_spark
class TestCleanNocRegions:

    def test_noc_column_is_renamed(
        self, spark: SparkSession, raw_noc_parquet: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "noc_silver"
        clean_noc_regions(spark, raw_noc_parquet, out)
        df = pd.read_parquet(out)
        assert "noc" in df.columns
        assert "NOC" not in df.columns

    def test_duplicates_are_removed(
        self, spark: SparkSession, raw_noc_parquet: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "noc_silver"
        clean_noc_regions(spark, raw_noc_parquet, out)
        df = pd.read_parquet(out)
        assert len(df[df["noc"] == "CHN"]) == 1
        assert len(df) == 3
