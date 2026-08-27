import os
from pyspark.sql import SparkSession, DataFrame
from logging import Logger
from pathlib import Path
from typing import Optional


def read_layer_tables(spark: SparkSession,
                      logger: Logger, 
                      layer_path: Path, 
                      tables_names: list[str]) -> Optional[dict[str, DataFrame]]:
    """ Read DataFrames package from Parquet format
    :param spark: Standard Spark Session
    :param logger: Standard Logger
    :param layer_path: Location of Parquet files path
    :param tables_names: List of DataFrames names
    :return: Dictionary with loaded DataFrames"""


    existing_dir_list = os.listdir(layer_path)
    expected_dir_list = tables_names
    output_dataframes: dict[str, DataFrame] = {}

    missing_tables = [
        table for table in expected_dir_list
        if table not in existing_dir_list
    ]

    if missing_tables:
        logger.error(f"Missing required directories: {missing_tables} in location: {layer_path}")
        return {}
        
    for table_name in expected_dir_list:
        table_path = layer_path / table_name

        if not os.path.exists(table_path):
            logger.error(f"DataFrame path doesn't exist: {table_path}")
            return {}

        try:
            logger.info(f"DataFrame {table_name} Parquet's set loading from location: {table_path}")
            dataframe = spark.read.load(str(table_path))
            dataframe.limit(1).collect()
            output_dataframes[table_name] = dataframe
            logger.info(f"Loaded: {table_name}")
        except Exception:
            logger.error(f"DataFrame {table_name} loading impossible from location: {table_path}")
            return {}
                
    return output_dataframes

def read_raw_data(expected_file_names: list[str], 
                  spark: SparkSession, 
                  logger: Logger, 
                  raw_folder_path: Path,
                  skip_invalid_tables: bool
                  ) -> dict[str, DataFrame]:
    """ Load each CSV data file and create individual DataFrame.
    :param expected_file_names: List of files names
    :param spark: Standard Spark Session
    :param logger: Standard Logger
    :param raw_folder_path: Folder with raw CSV files path
    :param skip_invalid_tables: If False, function raise Exception when file doesn't exist
    :return: Dictionary with loaded DataFrames"""        

    logger.info("Loading and checking Raw Files.")
    raw_dataframes_dict: dict[str, DataFrame] = {} 

    existing_files_names = os.listdir(raw_folder_path)

    for file_name in expected_file_names:
        if file_name in existing_files_names:
            table_name = file_name[:file_name.rfind(".")]                                      # File name without".csv"                  
            file_path = raw_folder_path / file_name                                            # Path + file name
            logger.info(f"Loading file: {file_path}")

            try:
                logger.info(f"Availability checking and basic loading CSV: {file_path}")
                dataframe = spark.read.csv(str(file_path), header = True) 
                dataframe.limit(1).collect()
                raw_dataframes_dict[table_name] = dataframe
                logger.info(f"CSV file correct: {file_path}") 
            except Exception:
                if not skip_invalid_tables:
                    logger.exception(f"No file or file corrupted: {file_path}")
                    raise
                else:
                    logger.warning(f"Skip corrupted or non-existent file: {file_path}")
                    

    loaded_table = list(raw_dataframes_dict.keys())

    if raw_dataframes_dict:
        logger.info(f"Loaded DataFrames: {loaded_table}")
    else:
        logger.info("No loaded DataFrames. Raw files corrupted or unavailable.")

    return raw_dataframes_dict