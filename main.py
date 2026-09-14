import src.utils.logger as L
import sys, time
logger = L.logging.getLogger(__name__)

from src.utils.spark_session import create_spark_session, stop_spark_session
from src.layers.bronze_layer import BronzeLayer
from src.layers.silver_layer import SilverLayer
from src.layers.gold_layer import GoldLayer
import src.utils.preflight as preflight
from src.utils.paths import INPUT_SOURCES_PATH

def main() -> None:
    logger.info("Start System")
    logger.info("Checking required components presence.")

    if not preflight.components_check(logger = logger,
                                      input_sources_path = INPUT_SOURCES_PATH):
        logger.error("Missing required components. Pipeline aborted.")
        logger.info("Stop System")
        raise RuntimeError("Preflight validation failed.")
    
    else: 
        logger.info("Components presence correct.")

    spark = None

    try:
        start_time = time.time()
        spark = create_spark_session(app_name = "Ecommerce Medallion Pipeline")

        bronze = BronzeLayer(
            spark = spark
        )
    
        logger.info("Start Pipeline Bronze.")
        bronze_run = bronze.run()

        if not bronze_run:
            sys.exit(1)

        silver = SilverLayer(
            spark = spark
        )

        logger.info("Start Pipeline Silver")
        silver_run = silver.run()

        if not silver_run:
            sys.exit(1) 

        gold = GoldLayer(
            spark = spark
        )
    
        logger.info("Start Pipeline Gold")
        gold_run = gold.run()

        if not gold_run:
            sys.exit(1)

        stop_time = time.time()
        pipeline_execution_time = round(stop_time - start_time, 2)

        logger.info(f"End Pipeline. Total execution time: {pipeline_execution_time}")
    finally:
        logger.info("Stopping Spark Session.")
        if spark is not None:
            stop_spark_session(spark = spark)


if __name__  == "__main__":
    main()
