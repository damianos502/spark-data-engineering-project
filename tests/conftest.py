import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ecommerce-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()
