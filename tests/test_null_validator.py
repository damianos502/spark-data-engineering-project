import logging, pytest, datetime
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, Row
import pyspark.sql.functions as SF
from src.validators.null_validator import null_search, columns_in_dataframe_presence, custom_drop_nulls, custom_fill_unknown, custom_fill_empty, custom_null_flagging, null_handling
from src.utils.spark_session import create_spark_session
from src.utils.paths import CONFIG_DIR

@pytest.fixture
def sample_dataframes_dict_generator():
    spark = create_spark_session("test_null_validator")
    dataframes_dict = {}
    sample_data = {
        "events": [("E00000001", "U009798", "P001393", "cart", "2025-07-08T14:28:55.893919"),
                   (None, "U005881", "P000669", "view", "2025-10-19T23:00:44.067982"),
                   (None, "U006348", "P001404", "view", None),
                   ("E00000004", "U002664", "P000400", None, "2025-07-19T22:47:07.019634"),
                   ("E00000005", None, "P000392", "view", "2024-10-24T10:20:33.602165")],
        "orders": [("00000001", None, "2025-09-09T14:52:37.292731", "processing", 689.66),
                    ("O00000002", "U003247", "2025-04-15T01:18:27.193404", None, 1666.85),
                    ("O00000003", "U007252", "2025-04-27T15:37:48.008624", "processing", 665.06),
                    ("O00000004", "U008986", None, "cancelled", 689.5),
                    ("O00000005", "U008537", "2024-11-13T08:15:18.498252", "cancelled", None)],
        "users": [("U000001", "Angel Hill", "donaldgarcia@example.net", "Other", "New Roberttown", None),
                  ("U000002", None, "jennifermiles@example.com", None, "South Bridget", "2024-03-05"),
                  (None, "Adam Shaffer", "jpeterson@example.org", "Male", "Curtisfurt", "2025-07-07"),
                  ("U000004", "Melanie Munoz", "blairamanda@example.com", "Other", "New Kellystad", "2024-03-07"),
                  ("U000005", "Janet Williams", None, "Female", "South Joshuastad", None)]           
                   }
    
    sample_schema = {
        "events": StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_timestamp", StringType(), True)
            ]),
        "orders": StructType([
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            ]),
        "users": StructType([
            StructField("user_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("city", StringType(), True),
            StructField("signup_date", StringType(), True)
            ])
    }

    for table_name, data in sample_data.items():
        dataframe = spark.createDataFrame(data, sample_schema[table_name])
        if "event_timestamp" in dataframe.columns:
            dataframe = dataframe.withColumn("event_timestamp", SF.to_timestamp("event_timestamp"))
        elif "signup_date" in dataframe.columns:
            dataframe = dataframe.withColumn("signup_date", SF.to_timestamp("signup_date"))
        elif "order_date" in dataframe.columns:
            dataframe = dataframe.withColumn("order_date", SF.to_timestamp("order_date"))

        dataframes_dict[table_name] = dataframe

    return dataframes_dict

@pytest.fixture
def null_search_result(sample_dataframes_dict_generator):
    logger = logging.getLogger("test_logger")
    result = null_search(input_dataframes = sample_dataframes_dict_generator, 
                         logger = logger)
    
    return result

def test_null_search(null_search_result):

    expected_nulls_dict = {
        "events": ['event_id', 'user_id', 'event_type', 'event_timestamp'],
        "orders": ['user_id', 'order_date', 'order_status', 'total_amount'],
        "users": ['user_id', 'name', 'email', 'gender', 'signup_date']           
                   }
    
    unexpected_nulls_dict = {
        "events": ['event_id', 'event_type', 'event_timestamp'],
        "orders": ['user_id', 'order_date', 'total_amount'],
        "users": ['user_id', 'email', 'gender', 'signup_date']
        }

    assert null_search_result == expected_nulls_dict
    assert null_search_result != unexpected_nulls_dict

def test_valid_columns_in_dataframe_presence(null_search_result, sample_dataframes_dict_generator):
    logger = logging.getLogger("test_logger")
    final_result = True
    for dataframe_name, columns_names in null_search_result.items():
        single_table_result = columns_in_dataframe_presence(logger = logger, 
                                                            dataframe = sample_dataframes_dict_generator[dataframe_name], 
                                                            dataframe_name = dataframe_name, 
                                                            columns_names = columns_names)
        if not single_table_result:
            final_result = False

    assert final_result == True

def test_invalid_columns_in_dataframe_presence(sample_dataframes_dict_generator):
    logger = logging.getLogger("test_logger")
    final_result = False

    incorrect_nulls_dict = {
        "events": ['event_id', 'event_type', 'event_timestamp', 'test'],
        "orders": ['user_id', 'order_date', 'total_amount', 'test'],
        "users": ['user_id', 'email', 'gender', 'signup_date', "test"]
        }

    for dataframe_name, columns_names in incorrect_nulls_dict.items():
        single_table_result = columns_in_dataframe_presence(logger = logger, 
                                                            dataframe = sample_dataframes_dict_generator[dataframe_name], 
                                                            dataframe_name = dataframe_name, 
                                                            columns_names = columns_names)

        if single_table_result == True:
            final_result = True
            break

    assert final_result == False

def test_drop_nulls(sample_dataframes_dict_generator):
    logger = logging.getLogger("test_logger")

    sample_dataframe = sample_dataframes_dict_generator["events"]
    dataframe_after_drop = sample_dataframe

    columns_for_drop = ["event_id", "user_id", "product_id", "event_timestamp"]

    for column_name in columns_for_drop:
        dataframe_after_drop = custom_drop_nulls(logger = logger,
                                                dataframe = dataframe_after_drop,
                                                dataframe_name = "events",
                                                column_name = column_name)
    

    expected_rows = [Row(event_id = 'E00000001', user_id = 'U009798', product_id = 'P001393', event_type = 'cart', event_timestamp = datetime.datetime(2025, 7, 8, 14, 28, 55, 893919)), 
                     Row(event_id = 'E00000004', user_id = 'U002664', product_id = 'P000400', event_type = None, event_timestamp = datetime.datetime(2025, 7, 19, 22, 47, 7, 19634))]
    final_rows = dataframe_after_drop.collect()[0:2]

    assert final_rows == expected_rows

def test_fill_unknown(sample_dataframes_dict_generator):
    logger = logging.getLogger("test_logger")

    sample_dataframe = sample_dataframes_dict_generator["events"]
    dataframe_after_fill = sample_dataframe

    columns_for_fill = ["event_id", "user_id", "product_id"]

    for column_name in columns_for_fill:
        dataframe_after_fill = custom_fill_unknown(logger = logger,
                                                    dataframe = dataframe_after_fill,
                                                    dataframe_name = "events",
                                                    column_name = column_name)
    
    expected_rows = [Row(event_id = 'E00000001', user_id = 'U009798', product_id = 'P001393', event_type = 'cart', event_timestamp = datetime.datetime(2025, 7, 8, 14, 28, 55, 893919)), 
                     Row(event_id = 'unknown', user_id = 'U005881', product_id = 'P000669', event_type = 'view', event_timestamp = datetime.datetime(2025, 10, 19, 23, 0, 44, 67982)), 
                     Row(event_id = 'unknown', user_id = 'U006348', product_id = 'P001404', event_type = 'view', event_timestamp = None), 
                     Row(event_id = 'E00000004', user_id = 'U002664', product_id = 'P000400', event_type = None, event_timestamp = datetime.datetime(2025, 7, 19, 22, 47, 7, 19634)), 
                     Row(event_id = 'E00000005', user_id = 'unknown', product_id = 'P000392', event_type = 'view', event_timestamp = datetime.datetime(2024, 10, 24, 10, 20, 33, 602165))]
    final_rows = dataframe_after_fill.collect()[0:5]

    assert final_rows == expected_rows

def test_fill_empty(sample_dataframes_dict_generator):
    logger = logging.getLogger("test_logger")

    sample_dataframe = sample_dataframes_dict_generator["events"]
    dataframe_after_fill = sample_dataframe

    columns_for_fill = ["event_id", "user_id", "product_id", "event_timestamp"]

    for column_name in columns_for_fill:
        dataframe_after_fill = custom_fill_empty(logger = logger,
                                                    dataframe = dataframe_after_fill,
                                                    dataframe_name = "events",
                                                    column_name = column_name)
    
    expected_rows = [Row(event_id='E00000001', user_id='U009798', product_id='P001393', event_type='cart', event_timestamp=datetime.datetime(2025, 7, 8, 14, 28, 55, 893919)), 
                     Row(event_id='', user_id='U005881', product_id='P000669', event_type='view', event_timestamp=datetime.datetime(2025, 10, 19, 23, 0, 44, 67982)), 
                     Row(event_id='', user_id='U006348', product_id='P001404', event_type='view', event_timestamp=None), 
                     Row(event_id='E00000004', user_id='U002664', product_id='P000400', event_type=None, event_timestamp=datetime.datetime(2025, 7, 19, 22, 47, 7, 19634)), 
                     Row(event_id='E00000005', user_id='', product_id='P000392', event_type='view', event_timestamp=datetime.datetime(2024, 10, 24, 10, 20, 33, 602165))]
    
    final_rows = dataframe_after_fill.collect()[0:5]

    assert final_rows == expected_rows

def test_null_flagging(sample_dataframes_dict_generator):
    logger = logging.getLogger("test_logger")

    sample_dataframe = sample_dataframes_dict_generator["events"]
    dataframe_after_flagging = sample_dataframe

    columns_for_flagging = ["event_id", "user_id", "product_id", "event_timestamp"]

    for column_name in columns_for_flagging:
        dataframe_after_flagging = custom_null_flagging(logger = logger,
                                                    dataframe = dataframe_after_flagging,
                                                    dataframe_name = "events",
                                                    column_name = column_name)
    
    expected_rows = [Row(event_id='E00000001', user_id='U009798', product_id='P001393', event_type='cart', event_timestamp=datetime.datetime(2025, 7, 8, 14, 28, 55, 893919), missing_values=None), 
                     Row(event_id=None, user_id='U005881', product_id='P000669', event_type='view', event_timestamp=datetime.datetime(2025, 10, 19, 23, 0, 44, 67982), missing_values='event_id'), 
                     Row(event_id=None, user_id='U006348', product_id='P001404', event_type='view', event_timestamp=None,  missing_values='event_id, event_timestamp'), 
                     Row(event_id='E00000004', user_id='U002664', product_id='P000400', event_type=None, event_timestamp=datetime.datetime(2025, 7, 19, 22, 47, 7, 19634), missing_values=None), 
                     Row(event_id='E00000005', user_id=None, product_id='P000392', event_type='view', event_timestamp=datetime.datetime(2024, 10, 24, 10, 20, 33, 602165), missing_values='user_id')]
    
    final_rows = dataframe_after_flagging.collect()[0:5]

    assert final_rows == expected_rows

def test_null_handling(sample_dataframes_dict_generator, null_search_result):
    logger = logging.getLogger("test_logger")
    final_rows_dicts = {}

    sample_null_handling_schema_path = "test_path"
    sample_null_handling_schema = {
        "events": {
        "event_id": "drop",
        "user_id": "drop",
        "product_id": "drop",
        "event_type": "drop",
        "event_timestamp": "drop"
        },
        "orders": {
        "order_id": "drop",
        "user_id": "flagging",
        "order_date": "flagging",
        "order_status": "fill_unknown",
        "total_amount": "flagging",
        },
        "users": {
        "user_id": "drop",
        "email": "fill_unknown",
        "gender": "fill_unknown",
        "name": "fill_empty",
        "city": "fill_empty"
        }
    }

    handling_result = null_handling(logger = logger,
                                    input_dataframes = sample_dataframes_dict_generator,
                                    dataframes_with_nulls = null_search_result,
                                    null_handling_schema = sample_null_handling_schema,
                                    null_handling_schema_path = sample_null_handling_schema_path
                                    )

    for table_name, dataframe in handling_result.items():
        final_rows_dicts[table_name] = dataframe.collect()[0:6]

    expected_rows = {'events': [Row(event_id='E00000001', user_id='U009798', product_id='P001393', event_type='cart', event_timestamp=datetime.datetime(2025, 7, 8, 14, 28, 55, 893919))], 
                     'orders': [Row(order_id='00000001', user_id=None, order_date=datetime.datetime(2025, 9, 9, 14, 52, 37, 292731), order_status='processing', total_amount=689.66, missing_values='user_id'), 
                                Row(order_id='O00000002', user_id='U003247', order_date=datetime.datetime(2025, 4, 15, 1, 18, 27, 193404), order_status='unknown', total_amount=1666.85, missing_values=None), 
                                Row(order_id='O00000003', user_id='U007252', order_date=datetime.datetime(2025, 4, 27, 15, 37, 48, 8624), order_status='processing', total_amount=665.06, missing_values=None), 
                                Row(order_id='O00000004', user_id='U008986', order_date=None, order_status='cancelled', total_amount=689.5, missing_values='order_date'), 
                                Row(order_id='O00000005', user_id='U008537', order_date=datetime.datetime(2024, 11, 13, 8, 15, 18, 498252), order_status='cancelled', total_amount=None, missing_values='total_amount')], 
                     'users':  [Row(user_id='U000001', name='Angel Hill', email='donaldgarcia@example.net', gender='Other', city='New Roberttown', signup_date=None), 
                                Row(user_id='U000002', name='', email='jennifermiles@example.com', gender='unknown', city='South Bridget', signup_date=datetime.datetime(2024, 3, 5, 0, 0)), 
                                Row(user_id='U000004', name='Melanie Munoz', email='blairamanda@example.com', gender='Other', city='New Kellystad', signup_date=datetime.datetime(2024, 3, 7, 0, 0)), 
                                Row(user_id='U000005', name='Janet Williams', email='unknown', gender='Female', city='South Joshuastad', signup_date=None)]}

    assert final_rows_dicts == expected_rows