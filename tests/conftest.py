"""Shared pytest fixtures."""
import os
import sys

import pytest
from pyspark.sql import SparkSession

# On Windows, PySpark requires winutils.exe + hadoop.dll from a Hadoop distribution.
# Tests that need Spark are skipped automatically if the setup is missing.
_HADOOP_HOME = os.environ.get("HADOOP_HOME", "")
_WINUTILS_OK = (
    sys.platform != "win32"
    or bool(_HADOOP_HOME and os.path.isfile(os.path.join(_HADOOP_HOME, "bin", "winutils.exe")))
)

requires_spark = pytest.mark.skipif(
    not _WINUTILS_OK,
    reason="Spark tests require HADOOP_HOME with winutils.exe on Windows",
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("OlympicPipelineTests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
