import logging
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

from src.validators.schema_validator import columns_validation, check_dataframes_schema


def test_invalid_columns_names(spark_session):
    # arrange
    test_targed_columns = {
        "orders": ["order_id", "user_id", "order_date"],
        "events": ["event_id", "user_id", "product_id"],
        "products": ["product_id", "product_name", "category"],
        "users": ["user_id", "name", "email", "gender"]
    }
    
    test_existing_columns = {
        "orders": ["order_id", "user_id", "order_date"],
        "events": ["event_id", "user_id", "product_id"],
        "products": ["product_id", "product_name", "category"],
        "users": ["invalid_user_id", "invalid_name", "invalid_email", "invalid_gender"]
    }

    spark = spark_session
    logger = logging.getLogger("test_logger")
    test_existing_dataframes = {}

    for dataframe_name, columns in test_existing_columns.items():
        schema = StructType([
                            StructField(column, StringType(), True)
                            for column in columns
                            ])
        
        dataframe = spark.createDataFrame(data = [], 
                                          schema = schema)
        
        test_existing_dataframes[dataframe_name] = dataframe

    
    # act + assert
    with pytest.raises(ValueError, match="Incorrect schema of columns in DataFrame: users"):
        columns_validation(
            raw_dataframes = test_existing_dataframes,
            logger = logger,
            expected_columns_dict = test_targed_columns,
            skip_invalid_tables = False
        )


def test_valid_column_names(spark_session):
    test_expected_columns = {
        "orders": ["order_id", "user_id", "order_date"],
        "events": ["event_id", "user_id", "product_id"],
        "products": ["product_id", "product_name", "category"],
        "users": ["user_id", "name", "email", "gender"]
    }

    spark = spark_session
    logger = logging.getLogger("test_logger")
    test_existing_dataframes = {}

    ### Existing Dataframes dictionary
    for dataframe_name, columns in test_expected_columns.items():
        schema = StructType([
                            StructField(column, StringType(), True)
                            for column in columns
                            ])
        
        dataframe = spark.createDataFrame(data = [], 
                                          schema = schema)
        
        test_existing_dataframes[dataframe_name] = dataframe

    # act
    result = columns_validation(
            raw_dataframes = test_existing_dataframes,
            logger = logger,
            expected_columns_dict = test_expected_columns,
            skip_invalid_tables = False
        )
    
    # assert
    assert result.keys() == test_existing_dataframes.keys()

    for table_name in test_existing_dataframes:
        assert result[table_name].columns == test_existing_dataframes[table_name].columns


def test_additional_column(spark_session):
    test_targed_columns = {
        "orders": ["order_id", "user_id", "order_date"],
        "events": ["event_id", "user_id", "product_id"],
        "products": ["product_id", "product_name", "category"],
        "users": ["user_id", "name", "email", "gender"]
    }

    test_existing_columns = {
        "orders": ["order_id", "user_id", "order_date", "additional"],
        "events": ["event_id", "user_id", "product_id", "additional"],
        "products": ["product_id", "product_name", "category", "additional"],
        "users": ["user_id", "name", "email", "gender", "additional"]
    }

    spark = spark_session
    logger = logging.getLogger("test_logger")

    test_existing_dataframes_dict = {}
    test_targed_dataframes_dict = {}

    ### Existing Dataframes dictionary
    for dataframe_name, columns in test_existing_columns.items():
        schema = StructType([
                            StructField(column, StringType(), True)
                            for column in columns
                            ])
        
        dataframe = spark.createDataFrame(data = [], 
                                          schema = schema)
        
        test_existing_dataframes_dict[dataframe_name] = dataframe


    ### Targed Dataframes dictionary
    for dataframe_name, columns in test_targed_columns.items():
        schema = StructType([
                            StructField(column, StringType(), True)
                            for column in columns
                            ])
        
        dataframe = spark.createDataFrame(data = [], 
                                          schema = schema)
        
        test_targed_dataframes_dict[dataframe_name] = dataframe

    ### Act
    validation_result = columns_validation(
            raw_dataframes = test_existing_dataframes_dict,
            logger = logger,
            expected_columns_dict = test_targed_columns,
            skip_invalid_tables = False
        )
        
    assert validation_result.keys() == test_targed_dataframes_dict.keys()

    for table_name in test_targed_dataframes_dict:
        assert validation_result[table_name].columns == test_targed_dataframes_dict[table_name].columns


def test_checking_valid_dataframes_schema(spark_session):
    spark = spark_session
    logger = logging.getLogger("test_logger")
    dataframes_dict = {}

    expected_schema = {
        "reviews": [["user_id", "string"],
                ["product_id", "int"],
                ["order_id", "double"],
                ],
                
        "events": [["event_type", "timestamp"],
                ["product_id", "string"],
                ["user_id", "int"],
                ],
        "users": [["gender", "string"],
                ["user_id", "double"],
                ["name", "string"],
                ],
    }

    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType()
    }

    for dataframe_name, schema in expected_schema.items():

        dataframe_schema = StructType([
                            StructField(column[0], type_map[column[1]], True)
                            for column in schema
                            ])

        dataframe = spark.createDataFrame(data = [],
                                          schema = dataframe_schema)
        dataframes_dict[dataframe_name] = dataframe

    result = check_dataframes_schema(silver_dataframes = dataframes_dict, 
                                     expected_schema = expected_schema, 
                                     logger = logger)
    assert result is True


def test_checking_invalid_dataframes_schema(spark_session):
    spark = spark_session
    logger = logging.getLogger("test_logger")
    dataframes_dict = {}

    invalid_schema = expected_schema = {
        "reviews": [["user_id", "int"],
                ["product_id", "string"],
                ["order_id", "timestamp"],
                ],
                
        "events": [["event_type", "string"],
                ["product_id", "string"],
                ["user_id", "double"],
                ],
        "users": [["gender", "string"],
                ["user_id", "double"],
                ["name", "string"],
                ],
    }

    expected_schema = {
        "reviews": [["user_id", "string"],
                ["product_id", "int"],
                ["order_id", "double"],
                ],
                
        "events": [["event_type", "timestamp"],
                ["product_id", "string"],
                ["user_id", "int"],
                ],
        "users": [["gender", "string"],
                ["user_id", "double"],
                ["name", "string"],
                ],
    }

    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType()
    }

    for dataframe_name, schema in invalid_schema.items():

        dataframe_schema = StructType([
                            StructField(column[0], type_map[column[1]], True)
                            for column in schema
                            ])

        dataframe = spark.createDataFrame(data = [],
                                          schema = dataframe_schema)
        dataframes_dict[dataframe_name] = dataframe

    result = check_dataframes_schema(silver_dataframes = dataframes_dict, 
                                     expected_schema = expected_schema, 
                                     logger = logger)
    assert result is False


def test_missing_column_in_schema(spark_session):
    spark = spark_session
    logger = logging.getLogger("test_logger")
    dataframes_dict = {}

    schema_with_missing_column = expected_schema = {
        "reviews": [["user_id", "string"],
                ["product_id", "int"],
                ["order_id", "double"],
                ],
                
        "events": [["event_type", "timestamp"],
                ["product_id", "string"],
                ["user_id", "int"],
                ]
    }

    expected_schema = {
        "reviews": [["user_id", "string"],
                ["product_id", "int"],
                ["order_id", "double"],
                ],
                
        "events": [["event_type", "timestamp"],
                ["product_id", "string"],
                ["user_id", "int"],
                ],
        "users": [["gender", "string"],
                ["user_id", "double"],
                ["name", "string"],
                ],
    }

    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType()
    }

    for dataframe_name, schema in schema_with_missing_column.items():

        dataframe_schema = StructType([
                            StructField(column[0], type_map[column[1]], True)
                            for column in schema
                            ])

        dataframe = spark.createDataFrame(data = [],
                                          schema = dataframe_schema)
        dataframes_dict[dataframe_name] = dataframe

    result = check_dataframes_schema(silver_dataframes = dataframes_dict, 
                                     expected_schema = expected_schema, 
                                     logger = logger)
    assert result is False
