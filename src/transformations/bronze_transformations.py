from pyspark.sql import DataFrame
import pyspark.sql.functions as SF
from logging import Logger
import uuid

def add_ingestion_columns(dataframes: dict[str, DataFrame], 
                          logger: Logger
                          ) -> dict[str, DataFrame]:
    """ Add ingesion columns to DataFrames
    :param dataframes: Dictionary with DataFrames
    :param logger: Standard Logger
    :return: DataFrames' Dictionary with addes ingestion columns"""
    
    ingested_dataframes: dict[str, DataFrame] = {}
    batch_id = str(uuid.uuid4())

    for name, dataframe in dataframes.items():
        ingested_dataframe = (dataframe
                                .withColumn("batch_id", SF.lit(batch_id))
                                .withColumn("ingestion_time", SF.current_timestamp())
                                )
        ingested_dataframes[name] = ingested_dataframe

    logger.info(f"Added ingestion columns for batch_id = {batch_id}")

    return ingested_dataframes   
