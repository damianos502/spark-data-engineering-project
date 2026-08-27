from pyspark.sql import DataFrame
import pyspark.sql.functions as SF
from logging import Logger


def null_search(input_dataframes: dict[str, DataFrame],
                logger: Logger
                ) -> dict[str, list[str]]:
    """ Checking if Null values exist in dataframes in individual columns. Return dictionary column name: list of columns with null values
    :param input_dataframes: Dictionary contain DataFrames submitted to searching
    :param logger: Standard Logger
    :return: Dictionary with column name and list of columns with null values"""


    dataframes_with_nulls: dict[str, list[str]] = {}

    for dataframe_name, dataframe in input_dataframes.items():
        logger.info(f"Checking null values: {dataframe_name}")
            
        columns_with_nulls: list[str] = []

        null_counts_row = (dataframe
                            .select([SF.count(
                                                SF.when(SF.col(column_name).isNull(), column_name)
                                            ).alias(column_name) 
                                    for column_name in dataframe.columns])
                            ).collect()[0]
  
        columns_with_nulls = [
            column_name
            for column_name in dataframe.columns
            if null_counts_row[column_name] > 0
        ]

        if columns_with_nulls:
            logger.info(f"{dataframe_name}: {columns_with_nulls} - Found Null Values!")
            dataframes_with_nulls[dataframe_name] = columns_with_nulls
        else:
            logger.info(f"{dataframe_name} - No Null Values")

    return dataframes_with_nulls

def columns_in_dataframe_presence(logger: Logger, 
                                  dataframe: DataFrame, 
                                  dataframe_name: str, 
                                  columns_names: list[str]
                                  ) -> bool:
    """ Checking if expected columns exist in DataFrame schema. Return True or False.
    :param logger: Standard Logger
    :param dataframe: DataFrame in which columns will be check
    :param dataframe_name: Name o checking DataFrame
    param: columns_names: List of names of expected columns
    :return: Schema validity"""

    missing_columns = []

    for name in columns_names:
        if name not in dataframe.columns:
            missing_columns.append(name)

    if missing_columns:
        logger.error(f"No column {columns_names} in DataFrame {dataframe_name}. Handling of null values impossible.")
        return False
    else:
        return True
    
def custom_drop_nulls(logger: Logger, 
                      dataframe: DataFrame, 
                      dataframe_name: str, 
                      column_name: str
                      ) -> DataFrame:
    """ Drop rows with null values. 
    :param logger: Standard Logger
    :param dataframe: Dataframe in which null values will be drop
    :param dataframe name: Name of handling DataFrame
    :param column_name: Name of handling column
    :return: Handled DataFrame"""

    logger.info(f"{dataframe_name}[{column_name}] - Dropped rows with null values.")
    return (dataframe
            .dropna(subset = [column_name])
            )

def custom_fill_unknown(logger: Logger, 
                        dataframe: DataFrame, 
                        dataframe_name: str, 
                        column_name: str
                        ) -> DataFrame:
    """ Replace null values with 'unknown' string. 
    :param logger: Standard Logger
    :param dataframe: Dataframe in which null values will be replace
    :param dataframe name: Name of handling DataFrame
    :param column_name: Name of handling column
    :return: Handled DataFrame"""

    logger.info(f"{dataframe_name}[{column_name}] - Null values replace - 'unknown'.")
    return (dataframe
            .fillna({column_name: 'unknown'})
            )

def custom_fill_empty(logger: Logger, 
                      dataframe: DataFrame, 
                      dataframe_name: str, 
                      column_name: str
                      ) -> DataFrame:
    """ Replace null values with empty cell. 
    :param logger: Standard Logger
    :param dataframe: Dataframe in which null values will be replace
    :param dataframe name: Name of handling DataFrame
    :param column_name: Name of handling column
    :return: Handled DataFrame"""

    logger.info(f"{dataframe_name}[{column_name}] - Null values replace - empty cell.")
    return (dataframe
            .fillna("", subset = [column_name])
            )

def custom_null_flagging(logger: Logger, 
                         dataframe: DataFrame, 
                         dataframe_name: str, 
                         column_name: str
                         ) -> DataFrame:
    """ Flags columns with null values in individual rows. Add column - columns with null
    :param logger: Standard Logger
    :param dataframe: Dataframe in which columns will be flagged
    :param dataframe name: Name of flagged DataFrame
    :param column_name: Name of flagged column
    :return: Flagged DataFrame"""

    logger.info(f"{dataframe_name}[{column_name}] - Added missing value flag.")

    if "missing_values" not in dataframe.columns:
        output_dataframe = (dataframe
                            .withColumn("missing_values", SF.when(SF.col(column_name).isNull(), SF.lit(column_name))
                                                        .otherwise(None)))
    else:
        output_dataframe = (dataframe
                            .withColumn("missing_values", SF.when((SF.col("missing_values").isNotNull()) & (SF.col(column_name).isNotNull()), SF.col("missing_values"))
                                                        .when((SF.col("missing_values").isNotNull()) & (SF.col(column_name).isNull()), SF.concat(SF.col("missing_values"), SF.lit(f", {column_name}")))
                                                        .when((SF.col("missing_values").isNull()) & (SF.col(column_name).isNull()), SF.lit(column_name))
                                                        .otherwise(SF.col("missing_values"))
                                        )
                            )

    return output_dataframe

HANDLING_MAP = {
            "drop": custom_drop_nulls,
            "fill_unknown": custom_fill_unknown,
            "fill_empty": custom_fill_empty,
            "flagging": custom_null_flagging
        }

def null_handling(logger: Logger, 
                  input_dataframes: dict[str, DataFrame], 
                  dataframes_with_nulls: dict[str, list],
                  null_handling_schema: dict[str, dict[str, str]],
                  null_handling_schema_path: str
                  ) -> dict[str, DataFrame]:
    """ Handle null values in individual columns.
    :param logger: Standard Logger
    :param input_dataframes: Dictionary contain DataFrames to handle
    :param dataframes_with_nulls: Dictionary contain DataFrames names and names of columns for handling
    :param null_handling_schema: Dictionary with Null Handling schemas
    :param null_handling_schema_path: Handling schema path
    :return: Dictionary with handled DataFrames"""


    output_dataframes: dict[str, DataFrame] = {}

    if not null_handling_schema:
        logger.error(f"Null values handling schema loading error: {null_handling_schema_path}")
        return {}

    output_dataframes = input_dataframes

    for dataframe_name, column_names in dataframes_with_nulls.items():
        cleaned_column_dataframe = input_dataframes[dataframe_name]

        columns_existing = columns_in_dataframe_presence(logger = logger,
                                                         dataframe = input_dataframes[dataframe_name],
                                                         dataframe_name = dataframe_name,
                                                         columns_names = column_names)
            
        if not columns_existing:
            logger.warning(f"Handling null values in DataFrame {dataframe_name} skipped.")
            continue

        for column_name in column_names:
            if column_name not in null_handling_schema[dataframe_name]:
                logger.warning(f"Found null values in {dataframe_name}/{column_name} - Missing handling schema!")
                continue

            handling_rule = null_handling_schema[dataframe_name][column_name]
            if handling_rule not in HANDLING_MAP:
                raise ValueError(f"Unknown null handling strategy: {handling_rule}")

            cleaned_column_dataframe = HANDLING_MAP[handling_rule](logger = logger, 
                                                                   dataframe = cleaned_column_dataframe, 
                                                                   dataframe_name = dataframe_name, 
                                                                   column_name = column_name)

        output_dataframes[dataframe_name] = cleaned_column_dataframe
        
    return output_dataframes

