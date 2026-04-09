import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "OlympicPipeline") -> SparkSession:
    """Return (or reuse) a local SparkSession."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )

    hadoop_home = os.environ.get("HADOOP_HOME", "")
    if hadoop_home:
        hadoop_bin = hadoop_home.replace("\\", "/") + "/bin"
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Djava.library.path={hadoop_bin}",
        )

    return builder.getOrCreate()
