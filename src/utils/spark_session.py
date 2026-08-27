from pyspark.sql import SparkSession
def create_spark_session(app_name: str) -> SparkSession:
    spark_session = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .getOrCreate()
        )
    
    return spark_session

def stop_spark_session(spark: SparkSession):
    spark.stop()