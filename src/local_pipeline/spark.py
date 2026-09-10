"""SparkSession initialization helper for the local PySpark streaming pipeline.

Configures environment variables, Windows Hadoop winutils, and Spark driver bindings.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from common.config import get_settings
from common.logger import get_logger

logger = get_logger("local-spark")


def setup_local_environment() -> None:
    """Configure JAVA_HOME, HADOOP_HOME, and PATH for local PySpark execution on Windows/Linux."""
    settings = get_settings()

    # 1. Resolve JAVA_HOME (from Settings, existing env, or known candidate paths)
    if settings.JAVA_HOME:
        configured_jdk = Path(os.path.expanduser(settings.JAVA_HOME)).resolve()
        if configured_jdk.is_dir():
            os.environ["JAVA_HOME"] = str(configured_jdk)
            logger.info("Configured JAVA_HOME from settings: %s", os.environ["JAVA_HOME"])

    if "JAVA_HOME" not in os.environ or not os.path.exists(os.environ["JAVA_HOME"]):
        candidate_jdks = [
            Path(os.path.expanduser("~/.jdks/openjdk-19.0.1")),
            Path(r"C:\Program Files\Java\jdk-17"),
            Path(r"C:\Program Files\Java\jdk-21"),
        ]
        for jdk in candidate_jdks:
            if jdk.is_dir():
                os.environ["JAVA_HOME"] = str(jdk.resolve())
                logger.info("Auto-configured fallback JAVA_HOME=%s", os.environ["JAVA_HOME"])
                break

    if "JAVA_HOME" in os.environ:
        java_bin = str((Path(os.environ["JAVA_HOME"]) / "bin").resolve())
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{java_bin};{os.environ.get('PATH', '')}"

    # 2. Configure HADOOP_HOME (from Settings, existing env, or repo hadoop/ dir on Windows)
    if settings.HADOOP_HOME:
        configured_hadoop = Path(os.path.expanduser(settings.HADOOP_HOME)).resolve()
        if configured_hadoop.is_dir():
            os.environ["HADOOP_HOME"] = str(configured_hadoop)

    if sys.platform == "win32" and (
        "HADOOP_HOME" not in os.environ or not os.path.exists(os.environ["HADOOP_HOME"])
    ):
        repo_root = Path(__file__).resolve().parent.parent.parent
        hadoop_dir = repo_root / "hadoop"
        if (hadoop_dir / "bin" / "winutils.exe").is_file():
            os.environ["HADOOP_HOME"] = str(hadoop_dir.resolve())

    if "HADOOP_HOME" in os.environ:
        hadoop_bin = str((Path(os.environ["HADOOP_HOME"]) / "bin").resolve())
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{hadoop_bin};{os.environ.get('PATH', '')}"

    # 3. Configure Python worker executable to match current virtual environment
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_local_spark_session(
    app_name: str = "FleetTelemetryLocalStreaming",
    include_kafka_package: bool = False,
) -> SparkSession:
    """Create or retrieve a local SparkSession optimized for local streaming.

    Args:
        app_name: Application name.
        include_kafka_package: If True, adds the spark-sql-kafka-0-10 package.

    Returns:
        Configured SparkSession instance.
    """
    setup_local_environment()

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.streaming.minBatchesToRetain", "2")
    )

    if include_kafka_package:
        import pyspark

        kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
        builder = builder.config("spark.jars.packages", kafka_pkg)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
