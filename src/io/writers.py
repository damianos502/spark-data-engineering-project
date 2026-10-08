from pyspark.sql import DataFrame
from logging import Logger
from src.utils.paths import DATA_DIR, ADDITIONAL_REPORTS
import json
from pathlib import Path


def write_parquet(data: dict[str, DataFrame] | DataFrame,
                   logger: Logger,
                   layer_name: str,
                   transformation_name: str | None = None
                   ) -> None:
    """ Export DataFrame or DataFrames to Parquet format.
    :param data: DataFrame or DataFrames intended to export
    :param logger: Standard Logger
    :param layer_name: Name of exporting layer
    :param transformation_name: Name of exporting transformation
    :return: None"""
    
    if isinstance(data, dict):
        for dataframe_name, dataframe in data.items():
              directory_path = DATA_DIR / layer_name / dataframe_name
              dataframe.write.mode("overwrite").parquet(str(directory_path))

        logger.info(f"{layer_name} layer's DataFrames export to Parquet format successfully")

    elif isinstance(data, DataFrame):
        directory_path = DATA_DIR / layer_name / transformation_name
        data.write.mode("overwrite").parquet(str(directory_path))

        logger.info(f"{layer_name} - {transformation_name} layer's DataFrame export to Parquet format successfully.")

def write_parquet_report(data: DataFrame,
                         logger: Logger,
                         report_name: str
                         ) -> None:
    """ Export DataFrame with report to Parquet format.
    :param data: DataFrame intended to export
    :param logger: Standard Logger
    :param report_name: Name of exporting report
    :return: None"""

    directory_path = ADDITIONAL_REPORTS / report_name
    data.write.mode("overwrite").parquet(str(directory_path))
    logger.info(f"{report_name} report export to Parquet format successfully.")


def write_json_report(new_data: dict,
               file_name: str, 
               logger: Logger):
    """ Create or append .json file with additional report
    :param new_data: Data for write or append
    :param file_name: Name for report file
    :param logger: Standard Logger"""


    full_file_name = file_name + ".json"
    path = ADDITIONAL_REPORTS / full_file_name

    if path.exists():
        with open(path, "r+", encoding = "utf-8") as json_file:
            loaded_data = json.load(json_file)
            loaded_data.append(new_data)
            data_to_write = loaded_data
            json_file.seek(0)
            json.dump(data_to_write, json_file, indent = 4)
            json_file.truncate()
            logger.info(f"New report was appended in path: {path}")
    else:
        with open(path, "w", encoding = "utf-8") as json_file:
            empty_list = []
            empty_list.append(new_data)
            data_to_write = empty_list
            json.dump(data_to_write, json_file, indent = 4)
            logger.info(f"File with additional report was created in path: {path}")

    
