"""Data quality checks applied between pipeline layers."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class QualityResult:
    check_name: str
    passed: bool
    details: str


class QualityError(Exception):
    """Raised when a critical data quality check fails."""


def check_not_empty(df: SparkDataFrame | pd.DataFrame, table_name: str) -> QualityResult:
    """Verify that a dataset is not empty."""
    if isinstance(df, pd.DataFrame):
        row_count = len(df)
    else:
        row_count = df.count()

    passed = row_count > 0
    result = QualityResult(
        check_name=f"{table_name}:not_empty",
        passed=passed,
        details=f"{row_count} rows",
    )
    _log_result(result)
    return result


def check_no_nulls(df: pd.DataFrame, columns: list[str], table_name: str) -> QualityResult:
    """Verify that specified columns contain no null values."""
    null_counts: dict[str, int] = {}
    for col in columns:
        if col in df.columns:
            count = int(df[col].isna().sum())
            if count > 0:
                null_counts[col] = count

    passed = len(null_counts) == 0
    details = "no nulls" if passed else f"nulls found: {null_counts}"
    result = QualityResult(
        check_name=f"{table_name}:no_nulls",
        passed=passed,
        details=details,
    )
    _log_result(result)
    return result


def check_unique(df: pd.DataFrame, columns: list[str], table_name: str) -> QualityResult:
    """Verify that the given columns form a unique key (no duplicates)."""
    duplicate_count = int(df.duplicated(subset=columns).sum())
    passed = duplicate_count == 0
    details = "all unique" if passed else f"{duplicate_count} duplicate rows on {columns}"
    result = QualityResult(
        check_name=f"{table_name}:unique",
        passed=passed,
        details=details,
    )
    _log_result(result)
    return result


def check_accepted_values(
    df: pd.DataFrame, column: str, accepted: set[str | None], table_name: str,
) -> QualityResult:
    """Verify that a column only contains values from an accepted set."""
    actual = set(df[column].dropna().unique())
    unexpected = actual - {v for v in accepted if v is not None}
    passed = len(unexpected) == 0
    details = "all valid" if passed else f"unexpected values: {unexpected}"
    result = QualityResult(
        check_name=f"{table_name}:accepted_values({column})",
        passed=passed,
        details=details,
    )
    _log_result(result)
    return result


def check_row_count_consistent(
    before: int, after: int, table_name: str, tolerance: float = 0.0,
) -> QualityResult:
    """Verify that row count did not change beyond a tolerance (0.0 = exact match)."""
    if before == 0:
        passed = True
        details = "no rows before"
    else:
        drop_ratio = 1.0 - (after / before)
        passed = drop_ratio <= tolerance
        details = f"before={before}, after={after}, drop={drop_ratio:.2%}"
    result = QualityResult(
        check_name=f"{table_name}:row_count_consistent",
        passed=passed,
        details=details,
    )
    _log_result(result)
    return result


def _log_result(result: QualityResult) -> None:
    if result.passed:
        logger.info("QUALITY PASSED  [%s] %s", result.check_name, result.details)
    else:
        logger.warning("QUALITY FAILED  [%s] %s", result.check_name, result.details)
