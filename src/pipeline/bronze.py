from pathlib import Path

from pyspark.sql import SparkSession

from src.utils.logger import get_logger

logger = get_logger(__name__)


def ingest_csv(spark: SparkSession, source_path: Path, output_path: Path) -> int:
    """Read a CSV file with Spark and write it as Parquet (bronze layer, no transformations).

    Parameters
    ----------
    spark:
        Active SparkSession.
    source_path:
        Path to the raw CSV file.
    output_path:
        Destination Parquet directory (bronze layer).

    Returns
    -------
    int
        Number of rows ingested.
    """
    logger.info("Ingesting %s -> %s", source_path.name, output_path)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(source_path.as_posix())
    )
    df.write.mode("overwrite").parquet(output_path.as_posix())
    row_count: int = df.count()
    logger.info("Bronze layer: wrote %s rows", f"{row_count:,}")
    return row_count
