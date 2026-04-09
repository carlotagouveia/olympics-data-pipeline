from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType

from src.utils.logger import get_logger

logger = get_logger(__name__)

_STRING_COLS: list[str] = [
    "name", "sex", "team", "noc", "games", "season", "city", "sport", "event",
]


def clean_athlete_events(spark: SparkSession, bronze_path: Path, silver_path: Path) -> int:
    """Clean athlete-events bronze Parquet and write silver Parquet.

    Parameters
    ----------
    spark:
        Active SparkSession.
    bronze_path:
        Bronze Parquet directory produced by the ingestion step.
    silver_path:
        Destination silver Parquet directory.

    Returns
    -------
    int
        Number of rows written.
    """
    df: DataFrame = spark.read.parquet(bronze_path.as_posix())

    df = (
        df
        .withColumnRenamed("ID",     "athlete_id")
        .withColumnRenamed("Name",   "name")
        .withColumnRenamed("Sex",    "sex")
        .withColumnRenamed("Age",    "age")
        .withColumnRenamed("Height", "height")
        .withColumnRenamed("Weight", "weight")
        .withColumnRenamed("Team",   "team")
        .withColumnRenamed("NOC",    "noc")
        .withColumnRenamed("Games",  "games")
        .withColumnRenamed("Year",   "year")
        .withColumnRenamed("Season", "season")
        .withColumnRenamed("City",   "city")
        .withColumnRenamed("Sport",  "sport")
        .withColumnRenamed("Event",  "event")
        .withColumnRenamed("Medal",  "medal")
    )

    df = (
        df
        .withColumn("athlete_id", F.col("athlete_id").cast(LongType()))
        .withColumn("year",       F.col("year").cast(LongType()))
        .withColumn("age",        F.col("age").cast(DoubleType()))
        .withColumn("height",     F.col("height").cast(DoubleType()))
        .withColumn("weight",     F.col("weight").cast(DoubleType()))
    )

    for col in _STRING_COLS:
        df = df.withColumn(col, F.trim(F.col(col)))

    # "NA" in the source means no medal — convert to a proper null
    df = df.withColumn(
        "medal",
        F.when(F.col("medal").isNull() | (F.col("medal") == "NA"), None)
         .otherwise(F.col("medal")),
    )

    before: int = df.count()
    df = df.dropDuplicates()
    removed: int = before - df.count()
    if removed:
        logger.info("Silver: removed %s duplicate rows", f"{removed:,}")

    df.write.mode("overwrite").parquet(silver_path.as_posix())
    row_count: int = df.count()
    logger.info("Silver layer (events): wrote %s rows", f"{row_count:,}")
    return row_count


def clean_noc_regions(spark: SparkSession, bronze_path: Path, silver_path: Path) -> int:
    """Clean NOC-regions bronze Parquet and write silver Parquet.

    Parameters
    ----------
    spark:
        Active SparkSession.
    bronze_path:
        Bronze Parquet directory.
    silver_path:
        Destination silver Parquet directory.

    Returns
    -------
    int
        Number of rows written.
    """
    df: DataFrame = spark.read.parquet(bronze_path.as_posix())
    df = df.withColumnRenamed("NOC", "noc")
    df = df.withColumn("noc", F.trim(F.col("noc")))
    df = df.dropDuplicates(["noc"])

    df.write.mode("overwrite").parquet(silver_path.as_posix())
    row_count: int = df.count()
    logger.info("Silver layer (noc): wrote %s rows", f"{row_count:,}")
    return row_count
