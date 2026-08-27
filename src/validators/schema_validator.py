from logging import Logger
from pyspark.sql import DataFrame
import pyspark.sql.functions as SF

def columns_validation(raw_dataframes: dict[str, DataFrame],
                       logger: Logger,
                       expected_columns_dict: dict[str, list[str]],
                       skip_invalid_tables: bool
                       ) -> dict[str, DataFrame]:
    """ Checking if DataFrame's columns have correct schema. Compare DataFrames schema with schema in expected_columns_dict parameter.
    :param raw_dataframes: Dictionary contain DataFrames for validation
    :param logger: Standard Logger
    :param expected_columns_dict: Dictionary contain DataFrame's name as key and list of columns names as value
    :param skip_invalid_tables: If False, function raise Exception when DataFrame's chema will be incorrect
    :return: Dictionary with correct DataFrames"""

    logger.info("Correctness of the columns schema validation.")
    output_dataframes: dict[str, DataFrame] = {}

    for name, dataframe in raw_dataframes.items():
        existing_colums = dataframe.columns

        if name not in expected_columns_dict:
            logger.warning(f"No excepted schema od columns for DataFrame: {name}")
            continue

        missing_columns = [
            column for column in expected_columns_dict[name]
            if column not in existing_colums
        ]

        if not missing_columns:
            fitted_dataframe = (dataframe
                                .select(*expected_columns_dict[name])
                                )
            output_dataframes[name] = fitted_dataframe
        else:
            if not skip_invalid_tables:
                logger.error(f"Incorrect schema of columns in DataFrame: {name}")
                raise ValueError(f"Incorrect schema of columns in DataFrame: {name}")
            else:
                logger.warning(f"Skip DataFrame with incorrect schema of columns: {name}")

    if output_dataframes:
        logger.info(f"Validation of columns schema finished. Saved DataFrames: {list(output_dataframes.keys())}")
    else:
        logger.info("No DataFrames with correct column schema.")

    return output_dataframes

def cast_column_type(input_dataframes: dict[str, DataFrame],
                     logger: Logger,
                     cast_schema: dict[str, dict[str, str]],
                     cast_schema_path: str
                    ) -> dict[str, DataFrame]:
    """ Load cast schema for columns from config file and cast types of columns
    :param input_dataframes: Dictionary contain DataFrames for cast
    :param logger: Standard Logger
    :param cast_schema: Dictionary contain DataFrame's name as key and dictionary with column name as key, column type as value
    :param cast_schema_path: Cast schema path
    :return: Dictionary with correct DataFrames"""
        

    if not cast_schema:
        logger.error(f"Cast schema loading error: {cast_schema_path}")
        return {}

    casted_dataframes: dict[str, DataFrame] = {}

    for dataframe_name, dataframe in input_dataframes.items():               
        info = f"DataFrame: {dataframe_name} - Columns types casting: "
        if dataframe_name in cast_schema:
            columns = cast_schema[dataframe_name]
        else:
            casted_dataframes[dataframe_name] = dataframe
            continue 

        for column_name, target_type in columns.items():
            if column_name in dataframe.columns:
                info = info + f"{column_name} -> {target_type}; "
                dataframe = (dataframe
                    .withColumn(column_name, SF.col(column_name).cast(target_type))
                    )
            else:
                logger.warning(f"No column {column_name} in schema for {dataframe_name}")
                    
        logger.info(info)    
        casted_dataframes[dataframe_name] = dataframe

    logger.info("Cast of types - Finished correctly.")
    return casted_dataframes

def check_dataframes_schema(silver_dataframes: dict[str, DataFrame],  
                            expected_schema: list[list[str, str]], 
                            logger: Logger
                            ) -> bool:
    """ Validate correctness of "DataFrames schemas
    :param silver_dataframes: Dictionary contain DataFrames for validation
    :param expected_schema: List of schemas for individual DataFrames
    :param logger: Standard Logger
    :return: True if schema correct"""    

    schema_error = False

    for dataframe_name, schema in expected_schema.items():
        if dataframe_name not in silver_dataframes.keys():
            logger.warning(f"No DataFrame {dataframe_name} in provided set of DataFrames!")
            schema_error = True
            continue
            
        actual_dataframe_schema = silver_dataframes[dataframe_name].dtypes

        for column in schema:
            column_tuple = tuple(column)
            if column_tuple not in actual_dataframe_schema:
                logger.info(f"Dataframes schema Error {dataframe_name}: {column_tuple}")
                schema_error = True

    if schema_error == True:
        logger.warning("Incorrect schema for provided Silver set.")
        return False 
    else:
        logger.info("Schema of provided Silver set is correct for Gold class.")
        return True