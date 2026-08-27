from pyspark.sql import DataFrame
from logging import Logger
from src.utils.paths import DATA_DIR


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