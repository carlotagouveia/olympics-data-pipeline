"""Unit tests for the bronze ingestion layer."""
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.pipeline.bronze import ingest_csv
from tests.conftest import requires_spark


@pytest.fixture()
def sample_csv(tmp_path: Path) -> Path:
    """Write a small CSV file for testing."""
    df = pd.DataFrame({
        "ID":   [1, 2, 3],
        "Name": ["Alice", "Bob", "Carol"],
        "NOC":  ["USA", "GBR", "CHN"],
    })
    path = tmp_path / "sample.csv"
    df.to_csv(path, index=False)
    return path


@requires_spark
class TestIngestCsv:

    def test_creates_parquet_output(self, spark: SparkSession, sample_csv: Path, tmp_path: Path) -> None:
        out = tmp_path / "output"
        ingest_csv(spark, sample_csv, out)
        assert out.exists()

    def test_returns_correct_row_count(self, spark: SparkSession, sample_csv: Path, tmp_path: Path) -> None:
        out = tmp_path / "output"
        count = ingest_csv(spark, sample_csv, out)
        assert count == 3

    def test_parquet_has_same_data_as_csv(self, spark: SparkSession, sample_csv: Path, tmp_path: Path) -> None:
        out = tmp_path / "output"
        ingest_csv(spark, sample_csv, out)

        original = pd.read_csv(sample_csv)
        result = pd.read_parquet(out)

        assert list(result.columns) == list(original.columns)
        assert len(result) == len(original)
        assert sorted(result["Name"].tolist()) == ["Alice", "Bob", "Carol"]

    def test_creates_output_directory_if_missing(
        self, spark: SparkSession, sample_csv: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "subdir" / "nested" / "output"
        ingest_csv(spark, sample_csv, out)
        assert out.exists()
