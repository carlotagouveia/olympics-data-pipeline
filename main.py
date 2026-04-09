import argparse
from datetime import date
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

from src.pipeline.bronze import ingest_csv
from src.pipeline.silver import clean_athlete_events, clean_noc_regions
from src.pipeline.gold import load_batch
from src.utils.spark import get_spark
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Olympic Data Pipeline")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Root data directory (default: ./data)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Append to the existing gold database instead of rebuilding from scratch.",
    )
    return parser.parse_args()


def run_pipeline(data_dir: Path, incremental: bool = False) -> None:
    raw_dir    = data_dir / "raw"
    bronze_dir = data_dir / "bronze"
    silver_dir = data_dir / "silver"
    gold_db    = data_dir / "gold" / "olympics.duckdb"

    if not incremental and gold_db.exists():
        gold_db.unlink()
        logger.info("Full-rebuild mode: removed existing gold database")

    spark: SparkSession = get_spark()

    logger.info("=== BRONZE LAYER ===")
    ingest_csv(spark, raw_dir / "athlete_events.csv", bronze_dir / "athlete_events")
    ingest_csv(spark, raw_dir / "noc_regions.csv",    bronze_dir / "noc_regions")

    logger.info("=== SILVER LAYER ===")
    clean_athlete_events(spark, bronze_dir / "athlete_events", silver_dir / "athlete_events")
    clean_noc_regions(spark,    bronze_dir / "noc_regions",    silver_dir / "noc_regions")

    logger.info("=== GOLD LAYER ===")

    # Read silver with Spark, then convert to pandas for SCD batch processing
    all_events: pd.DataFrame = (
        spark.read
        .parquet((silver_dir / "athlete_events").as_posix())
        .toPandas()
    )
    noc_path = silver_dir / "noc_regions"

    editions: list[str] = sorted(
        all_events["games"].dropna().unique().tolist(),
        key=lambda g: int(str(g).split(" ")[0]) if str(g).split(" ")[0].isdigit() else 0,
    )
    logger.info("Processing %s Olympic editions as individual batches", len(editions))

    for games_name in editions:
        batch: pd.DataFrame = all_events[all_events["games"] == games_name].copy()
        load_year: int = int(batch["year"].iloc[0])
        batch_load_date: date = date(load_year, 1, 1)

        batch_path = silver_dir / f"_batch_{games_name.replace(' ', '_')}.parquet"
        batch.to_parquet(batch_path, index=False, engine="pyarrow")

        logger.info("  Batch: %s (%s rows)", games_name, f"{len(batch):,}")
        load_batch(batch_path, noc_path, gold_db, batch_load_date)
        batch_path.unlink(missing_ok=True)

    spark.stop()
    logger.info("=== PIPELINE COMPLETE ===")
    logger.info("Gold database: %s", gold_db.resolve())


def main() -> None:
    args = _parse_args()
    run_pipeline(args.data_dir, incremental=args.incremental)


if __name__ == "__main__":
    main()
