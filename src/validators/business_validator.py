from pyspark.sql import DataFrame, SparkSession
from logging import Logger
from dataclasses import dataclass
from typing import Optional
import pyspark.sql.functions as SF
from functools import reduce

@dataclass
class ValidationContext:
    logger: Logger
    spark: SparkSession
    input_dataframes: dict[str, DataFrame]
    enums_schema: dict[str, list[str]]


def get_method(rule_value: str) -> Optional[str]:
    """ Convert predefined names od rules to names in Method Map
    :param rule_value: Predefine name of rule for validate.
    :return: Converted name of rule"""
    if rule_value.startswith("df_"):
        return "row_in_df"
    elif rule_value == "in_enum":
        return "in_enum"
    elif rule_value == "is_past":
        return "is_past"
    elif rule_value == "is_positive":
        return "is_positive"
    else:
        return None

def row_in_df(dataframe_to_validate: DataFrame, 
              column_name: str, 
              rule_value: str,
              context: ValidationContext
              ) -> Optional[tuple[DataFrame, str]]:
    """ Check whether the foreign key column has a reference to dataframe with matching primary key. Add column with validation result (Is value exist in joined DataFrame as a primary key).
    :param dataframe_to_validate: Dataframe with foreign key column
    :param column_name: Key column name
    :param rule_value: Name of rule for validate
    :param context: Standard function context
    :return: Validated dataframe, Added column name"""

    right_dataframe_name = rule_value.replace("df_", "")
    right_dataframe: DataFrame = context.input_dataframes.get(right_dataframe_name)

    if right_dataframe is None:
        context.logger.info(f"No table with name {right_dataframe_name} exist.")
        return None, None

    if column_name not in right_dataframe.columns:
        context.logger.info(f"No column {column_name} in DataFrame {right_dataframe_name}.")
        return None, None
    else:
        # Create new dataframe. Join two dataframes on key column name. Add column with value True if value from left dataframe's foreign key exist in right dataframe's primary key  
        output_dataframe = (dataframe_to_validate.alias("l")
                .join(right_dataframe.alias("r"), on = column_name, how = "left")
                .withColumn(f"df_{right_dataframe_name}_ref", SF.when(SF.col(f"r.{column_name}").isNotNull(), True).otherwise(False))
                .select("l.*", f"df_{right_dataframe_name}_ref")
                )  
                
        return output_dataframe, f"df_{right_dataframe_name}_ref"
    
def in_enum(dataframe_to_validate: DataFrame, 
            column_name: str,
            rule_value: str,
            context: ValidationContext 
            ) -> Optional[tuple[DataFrame, str]]:
    
    """ Check if value in column exist in predefined categories range. Add column with validation result (Is value exist in predefined range).
    :param dataframe_to_validate: Dataframe containing values ​​to be checked
    :param column_name: Name of the column containing values ​​to be checked 
    :param context: Standard function context
    :return: Validated dataframe, Added column name"""


    if column_name not in context.enums_schema:
        context.logger.info(f"No validation key for {column_name} in enums schema.")
        return None, None
            
    correct_range = context.spark.createDataFrame(data = [(x,) for x in context.enums_schema[column_name]], schema = [column_name])

    # Create new dataframe. Join two dataframes on key column name. Add column with value True if value from left dataframe's foreign key is not match with any value from right dataframe
    output_dataframe = (dataframe_to_validate.alias("l")
            .join(SF.broadcast(correct_range.alias("r")), on = column_name, how = "left")
            .withColumn(f"correct_{column_name}", SF.when(SF.col(f"r.{column_name}").isNotNull(), True).otherwise(False))
            .select("l.*", f"correct_{column_name}")
            )
            
    return output_dataframe, f"correct_{column_name}"

def is_past(dataframe_to_validate: DataFrame, 
            column_name: str,
            rule_value: str,
            context: ValidationContext
            ) -> DataFrame:
    """ Check whether the values with timestamp type are in the past. Add column with validation result (Is timestamp in past)
    :param dataframe_to_validate: Dataframe containing values ​​to be checked
    :param column_name: Name of the column containing values ​​to be checked 
    :return: Validated dataframe, Added column name"""

    # Create new dataframe. Add column with value True if timestamp is past.
    output_dataframe = (dataframe_to_validate
                        .withColumn(f"correct_{column_name}", SF.when(SF.col(column_name) < SF.current_timestamp(), True).otherwise(False))
                        )
            
    return output_dataframe, f"correct_{column_name}"

def is_positive(dataframe_to_validate: DataFrame, 
                column_name: str,
                rule_value: str,
                context: ValidationContext
                ) -> DataFrame:
    """ Check whether the values are positive. Add column with validation result (Is value positive)
    :param dataframe_to_validate: Dataframe containing values ​​to be checked
    :param column_name: Name of the column containing values ​​to be checked 
    :return: Validated dataframe, Added column name"""

    # Create new dataframe. Add column with value True if value is positive.
    output_dataframe = (dataframe_to_validate
                        .withColumn(f"correct_{column_name}", SF.when(SF.col(column_name) > 0, True).otherwise(False))
                        )
            
    return output_dataframe, f"correct_{column_name}"

def business_flagging(input_dataframes: dict[str, DataFrame],
                      logger: Logger,
                      spark: SparkSession,
                      business_rules_schema: dict[str, dict[str, str]],
                      business_rules_schema_path: str,
                      enums_schema: dict[str, list[str]],
                      enums_schema_path: str
                      ) -> dict[str, DataFrame]:
    """ Flagging business data alignment. Return dictionary with dataframes. Dataframes has added columns with validations results individual columns and column with overall validation result.
    :param input_dataframes: Dictionary of dataframes intended for validation
    :param logger: Standard Logger
    :param spark: Spark Session
    :param business_rules_schema: Dictionary of validation rules for individual columns
    :param business_rules_schema_path: Path of file with business rules
    :param enums_schema: Dictionary of enums schema for individual columns
    :param business_rules_schema_path: Path of file with enums rules
    :return: Dictionary with validated Dataframes"""

    if not business_rules_schema:
        logger.error(f"Business rules schema loading error: {business_rules_schema_path}")
        return {}

    if not enums_schema:
        logger.error(f"Business schema loading error {enums_schema_path}")
        return {}

    final_output_dataframes_dict: dict[str, DataFrame] = {}

    context = ValidationContext(logger = logger,
                                spark = spark,
                                input_dataframes = input_dataframes,
                                enums_schema = enums_schema)

    # Method map with references to functions
    METHOD_MAP = {
        "row_in_df": row_in_df,
        "in_enum": in_enum,
        "is_past": is_past,
        "is_positive": is_positive,
    }

    for dataframe_name, dataframe in input_dataframes.items():
        added_columns: list[str] = []
        added_column_name: str = ""
        checked_dataframe: DataFrame = dataframe

        if dataframe_name not in business_rules_schema:
            logger.warning(f"No business rules for DataFrame {dataframe_name} in loaded schema.")
            continue

        current_dataframe_rules = business_rules_schema[dataframe_name]

        for column_name in dataframe.columns:
            if column_name not in current_dataframe_rules:
                continue 
            rule_value = current_dataframe_rules[column_name]
            method_key = get_method(rule_value)

            if not method_key:
                logger.warning(f"Incorrect name of validation rule - {rule_value} for {dataframe_name}/{column_name}")
                continue

            validation_func = METHOD_MAP[method_key]
            checked_dataframe, added_column_name = validation_func(checked_dataframe, 
                                                                     column_name, 
                                                                     rule_value, 
                                                                     context)
            if checked_dataframe is None or added_column_name is None:
                logger.info(f"{dataframe_name}/{column_name}: Skip validation for rule - {rule_value}")
                continue

            added_columns.append(added_column_name)

        # Business overall correct if no validation column has been added
        if not added_columns:
            sum_up_dataframe = (checked_dataframe
                                .withColumn("business_overall_correct",
                                            SF.lit(True)))

        else:
            # Business overall correct is True if all of added columns are True
            columns_expr = [
                SF.coalesce(SF.col(column), SF.lit(False))
                for column in added_columns
            ]

            sum_up_expr = reduce(
                lambda a, b: a & b,
                columns_expr 
            )

            sum_up_dataframe = (checked_dataframe
                            .withColumn("business_overall_correct", sum_up_expr))

        final_output_dataframes_dict[dataframe_name] = sum_up_dataframe

            
    return final_output_dataframes_dict