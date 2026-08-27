from pyspark.sql import DataFrame
from typing import Optional
from logging import Logger


def empty_content_check(dataframes: dict[str, DataFrame],
                        logger: Logger,
                        skip_invalid_tables: bool
                        ) -> Optional[dict[str, DataFrame]]:
    """ Check if DataFrames are not empty. Return Dictionary with not empty DataFrames or None if all of tables are empty
    :param dataframes: Dictionary with DataFrames to check
    :param logger: Standard Logger
    :param skip_invalid_tables: If False, function raise Exception when DataFrame will be empty
    :return: Validated Dictionary of not empty DataFrames"""
    
    output_dataframes: dict[str, DataFrame] = {}

    logger.info("Checking if DataFrames are not empty.")

    for name, dataframe in dataframes.items():
        if dataframe.isEmpty() == True:
            if not skip_invalid_tables:
                logger.error(f"Dataframe {name} empty!")
                raise ValueError(f"Dataframe {name} empty.")
            else:
                logger.info(f"Skip empty DataFrame {name}.")
        else:
            output_dataframes[name] = dataframe

    if output_dataframes:
        logger.info(f"Checking if DataFrames are empty finished. Saved DataFrames: {list(output_dataframes.keys())}")
    else:
        logger.info("All of DataFrames are empty.")

    return output_dataframes