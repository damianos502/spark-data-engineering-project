from pyspark.sql import DataFrame
import pyspark.sql.functions as SF
from pyspark.sql.window import Window
from logging import Logger


def duplicates_handling(logger: Logger, 
                        input_dataframes: dict[str, DataFrame],
                        duplicate_schema: dict[str, list[str]], 
                        duplicate_schema_path: str, 
                        ) -> tuple[dict[str, DataFrame], dict[str, DataFrame]]:
    """ Handle duplicates in individual Columns using deduplicate schema. 
    :param logger: Standard Logger
    :param input_dataframes: Dictionary contain DataFrames to deduplicate
    :param duplicate_schema: Dictionary with DataFrames deduplicate schemas
    :param deuplicate_schema_path: Deduplicate schema path
    :return: Dictionary with deduplicated DataFrames, Dictionary with DataFrames with dropped rows"""

    output_dataframes = input_dataframes
    dropped_rows_dict: dict[str, DataFrame] = {}

    if not duplicate_schema:
        logger.error(f"Deduplicate schema loading error: {duplicate_schema_path}")
        return {}, {}
        
    for dataframe_name, dataframe in input_dataframes.items():

        if dataframe_name not in duplicate_schema:
            logger.info(f"No correct deduplicate schema for DataFrame: {dataframe_name}")
            continue

        if not set(duplicate_schema[dataframe_name]).issubset(dataframe.columns):
            logger.info(f"Columns in deduplicate schema and columns in DataFrame {dataframe_name} incompatible.")
            continue
            
        columns_for_deduplicate = duplicate_schema[dataframe_name]

        # Expressions for calculate individual columns score    
        not_null_value_expressions_list = [SF.when(SF.col(column_name).isNotNull(), SF.lit(1))
                                            .otherwise(SF.lit(0))
                                            for column_name in dataframe.columns]

        # Calculate overall score for individual row
        single_row_score = not_null_value_expressions_list[0]
        for value_existing in not_null_value_expressions_list[1:]:
            single_row_score = single_row_score + value_existing

        # Create windows partition by individual columns with dupliocates
        ranking_window = Window.partitionBy(*columns_for_deduplicate).orderBy(
            SF.col("row_score").desc(),
            SF.col("ingestion_time").desc()
        )

        count_window = Window.partitionBy(*columns_for_deduplicate)

        base_dataframe = (dataframe
                            .withColumn("row_score", single_row_score)
                            .withColumn("duplicate_count", SF.count("*").over(count_window))
                            .withColumn("row_rank", SF.row_number().over(ranking_window))
                            )

        # Dropped rows where row_rank > 1
        dropped_rows = (base_dataframe
                                .filter(SF.col("row_rank") > 1)
                                .drop("row_score", "duplicate_count", "row_rank"))
        

        dropped_rows_dict[dataframe_name] = dropped_rows

        # Dataframes after deduplicates
        deduplicates_dataframe = (base_dataframe
                            .filter(SF.col("row_rank") == 1)
                            .drop("row_score", "duplicate_count", "row_rank")
                            )
            
        output_dataframes[dataframe_name] = deduplicates_dataframe
            

    return output_dataframes, dropped_rows_dict