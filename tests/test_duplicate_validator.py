import datetime
import logging

import pytest
import pyspark.sql.functions as SF
from src.validators.duplicate_validator import duplicates_handling
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, Row
from pyspark.testing.utils import assertDataFrameEqual


duplicates_schema = {
    "events": ["event_id"],

    "order_items": ["order_item_id"],

    "orders": ["order_id"],

    "products": ["product_id"],

    "reviews": ["review_id"],

    "users": ["user_id"]
}

@pytest.fixture
def sample_dataframes_without_duplicates_generator(spark_session):
    spark = spark_session
    dataframes_dict = {}
    sample_data = {
        "events": [("E00000001", "U009798", "P001393", "cart", "2025-07-08T14:28:55.893919", "2025-07-07T14:28:55.893919"),
                   ("E00000002", "U005881", "P000669", "view", "2025-10-19T23:00:44.067982", "2025-07-08T14:28:55.893919"),
                   ("E00000003", "U006348", "P001404", "view", "2025-05-09T07:02:42.256662", "2025-07-09T14:28:55.893919"),
                   ("E00000004", "U002664", "P000400", "cart", "2025-07-19T22:47:07.019634", "2025-07-10T14:28:55.893919"),
                   ("E00000005", "U005776", "P000392", "view", "2024-10-24T10:20:33.602165", "2025-07-11T14:28:55.893919")],
        "orders": [("00000001", "U009310", "2025-09-09T14:52:37.292731", "processing", 689.66, "2025-07-07T14:28:55.893919"),
                    ("O00000002", "U003247", "2025-04-15T01:18:27.193404", "completed", 1666.85, "2025-07-08T14:28:55.893919"),
                    ("O00000003", "U007252", "2025-04-27T15:37:48.008624", "processing", 665.06, "2025-07-09T14:28:55.893919"),
                    ("O00000004", "U008986", "2025-10-04T20:35:22.204857", "cancelled", 689.5, "2025-07-10T14:28:55.893919"),
                    ("O00000005", "U008537", "2024-11-13T08:15:18.498252", "cancelled", 860.5, "2025-07-11T14:28:55.893919")],
        "users": [("U000001", "Angel Hill", "donaldgarcia@example.net", "Other", "New Roberttown", "2025-03-13", "2025-07-08T14:28:55.893919"),
                  ("U000002", "Jesse Guzman", "jennifermiles@example.com", "Male", "South Bridget", "2024-03-05", "2025-07-09T14:28:55.893919"),
                  ("U000003", "Adam Shaffer", "jpeterson@example.org", "Male", "Curtisfurt", "2025-07-07", "2025-07-10T14:28:55.893919"),
                  ("U000004", "Melanie Munoz", "blairamanda@example.com", "Other", "New Kellystad", "2024-03-07", "2025-07-11T14:28:55.893919"),
                  ("U000005", "Janet Williams", "kendragalloway@example.org", "Female", "South Joshuastad", "2025-01-29", "2025-07-12T14:28:55.893919")]           
                   }
    
    sample_schema = {
        "events": StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("ingestion_time", StringType())
            ]),
        "orders": StructType([
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("ingestion_time", StringType())
            ]),
        "users": StructType([
            StructField("user_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("city", StringType(), True),
            StructField("signup_date", StringType(), True),
            StructField("ingestion_time", StringType())
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
        elif "ingestion_time" in dataframe.columns:
            dataframe = dataframe.withColumn("ingestion_time", SF.to_timestamp("ingestion_time" ))

        dataframes_dict[table_name] = dataframe

    return dataframes_dict

@pytest.fixture
def sample_dataframes_with_duplicates_generator(spark_session):
    spark = spark_session
    dataframes_dict = {}
    sample_data = {
        "events": [("E00000001", "U009798", "P001393", None, None, "2025-07-07T14:28:55.893919"),
                   ("E00000002", None, "P000669", "view", "2025-10-19T23:00:44.067982", "2025-07-08T14:28:55.893919"),
                   ("E00000003", "U006348", "P001404", "view", "2025-05-09T07:02:42.256662", "2025-07-09T14:28:55.893919"),
                   ("E00000002", "U002664", "P000400", "cart", None, "2025-07-10T14:28:55.893919"),
                   ("E00000002", "U005776", None, "view", "2024-10-24T10:20:33.602165", "2025-07-11T14:28:55.893919")],
        "orders": [("00000001", None, "2025-09-09T14:52:37.292731", "processing", 689.66, "2025-07-07T14:28:55.893919"),
                    ("O00000001", "U003247", None, "completed", None, "2025-07-08T14:28:55.893919"),
                    ("O00000005", "U007252", "2025-04-27T15:37:48.008624", "processing", 665.06, "2025-07-09T14:28:55.893919"),
                    ("O00000004", "U008986", "2025-10-04T20:35:22.204857", None, 689.5, "2025-07-10T14:28:55.893919"),
                    ("O00000005", "U008537", "2024-11-13T08:15:18.498252", "cancelled", 860.5, "2025-07-11T14:28:55.893919")],
        "users": [("U000001", None, "donaldgarcia@example.net", "Other", "New Roberttown", "2025-03-13", "2025-07-07T14:28:55.893919"),
                  ("U000002", "Jesse Guzman", "jennifermiles@example.com", "Male", "South Bridget", "2024-03-05", "2025-07-08T14:28:55.893919"),
                  ("U000001", "Adam Shaffer", None, "Male", "Curtisfurt", None, None),
                  ("U000004", "Melanie Munoz", None, "Other", "New Kellystad", "2024-03-07","2025-07-10T14:28:55.893919"),
                  ("U000001", "Janet Williams", "kendragalloway@example.org", None, "South Joshuastad", "2025-01-29", "2025-07-11T14:28:55.893919")]           
                   }
    
    sample_schema = {
        "events": StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_timestamp", StringType(), True), 
            StructField("ingestion_time", StringType())
            ]),
        "orders": StructType([
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("ingestion_time", StringType())
            ]),
        "users": StructType([
            StructField("user_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("city", StringType(), True),
            StructField("signup_date", StringType(), True), 
            StructField("ingestion_time", StringType())
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
        elif "ingestion_time" in dataframe.columns:
            dataframe = dataframe.withColumn("ingestion_time", SF.to_timestamp("ingestion_time" ))

        dataframes_dict[table_name] = dataframe

    return dataframes_dict


def test_invalid_duplicate_handling_schema():
    logger = logging.getLogger("Test_logger")
    input_dataframes = {}
    result1, result2 = duplicates_handling(logger = logger,
                                           input_dataframes = input_dataframes,
                                           duplicate_schema = {},
                                           duplicate_schema_path = "test_path") 
    
    assert not result1
    assert not result2 


def test_dataframe_without_duplicates(sample_dataframes_without_duplicates_generator):
    logger = logging.getLogger("Test_logger")

    deduplicates_dataframes, rejected_rows = duplicates_handling(logger = logger,
                                                                 input_dataframes = sample_dataframes_without_duplicates_generator,
                                                                 duplicate_schema = duplicates_schema,
                                                                 duplicate_schema_path = "test_path")
    
    for dataframe in rejected_rows.values():
        assert dataframe.isEmpty()

    assert (
        sample_dataframes_without_duplicates_generator.keys()
        == deduplicates_dataframes.keys()
    )

    for name, expected_df in sample_dataframes_without_duplicates_generator.items():
        assertDataFrameEqual(expected_df, deduplicates_dataframes[name])
        


def test_dataframe_with_duplicates(sample_dataframes_with_duplicates_generator):
    logger = logging.getLogger("Test_logger")

    deduplicates_dataframes_dict = {}
    rejected_rows_dict = {}

    deduplicates_dataframes, rejected_rows = duplicates_handling(logger = logger,
                                                                 input_dataframes = sample_dataframes_with_duplicates_generator,
                                                                 duplicate_schema = duplicates_schema,
                                                                 duplicate_schema_path = "test_path")
    
    for table_name, dataframe in deduplicates_dataframes.items():
        deduplicates_dataframes_dict[table_name] = dataframe.collect()

    for table_name, dataframe in rejected_rows.items():
        rejected_rows_dict[table_name] = dataframe.collect()


    expected_deduplicates_rows_dict = {'events': [Row(event_id='E00000001', user_id='U009798', product_id='P001393', event_type=None, event_timestamp=None, ingestion_time='2025-07-07T14:28:55.893919'), 
                                                  Row(event_id='E00000002', user_id='U005776', product_id=None, event_type='view', event_timestamp=datetime.datetime(2024, 10, 24, 10, 20, 33, 602165), ingestion_time='2025-07-11T14:28:55.893919'), 
                                                  Row(event_id='E00000003', user_id='U006348', product_id='P001404', event_type='view', event_timestamp=datetime.datetime(2025, 5, 9, 7, 2, 42, 256662), ingestion_time='2025-07-09T14:28:55.893919')], 
                                       'orders': [Row(order_id='00000001', user_id=None, order_date=datetime.datetime(2025, 9, 9, 14, 52, 37, 292731), order_status='processing', total_amount=689.66, ingestion_time='2025-07-07T14:28:55.893919'), 
                                                  Row(order_id='O00000001', user_id='U003247', order_date=None, order_status='completed', total_amount=None, ingestion_time='2025-07-08T14:28:55.893919'), 
                                                  Row(order_id='O00000004', user_id='U008986', order_date=datetime.datetime(2025, 10, 4, 20, 35, 22, 204857), order_status=None, total_amount=689.5, ingestion_time='2025-07-10T14:28:55.893919'), 
                                                  Row(order_id='O00000005', user_id='U008537', order_date=datetime.datetime(2024, 11, 13, 8, 15, 18, 498252), order_status='cancelled', total_amount=860.5, ingestion_time='2025-07-11T14:28:55.893919')], 
                                       'users': [Row(user_id='U000001', name='Janet Williams', email='kendragalloway@example.org', gender=None, city='South Joshuastad', signup_date=datetime.datetime(2025, 1, 29, 0, 0), ingestion_time='2025-07-11T14:28:55.893919'), 
                                                 Row(user_id='U000002', name='Jesse Guzman', email='jennifermiles@example.com', gender='Male', city='South Bridget', signup_date=datetime.datetime(2024, 3, 5, 0, 0), ingestion_time='2025-07-08T14:28:55.893919'), 
                                                 Row(user_id='U000004', name='Melanie Munoz', email=None, gender='Other', city='New Kellystad', signup_date=datetime.datetime(2024, 3, 7, 0, 0), ingestion_time='2025-07-10T14:28:55.893919')]}

    expected_rejected_rows_dict = {'events': [Row(event_id='E00000002', user_id='U002664', product_id='P000400', event_type='cart', event_timestamp=None, ingestion_time='2025-07-10T14:28:55.893919'), 
                                              Row(event_id='E00000002', user_id=None, product_id='P000669', event_type='view', event_timestamp=datetime.datetime(2025, 10, 19, 23, 0, 44, 67982), ingestion_time='2025-07-08T14:28:55.893919')],
                                   'orders': [Row(order_id='O00000005', user_id='U007252', order_date=datetime.datetime(2025, 4, 27, 15, 37, 48, 8624), order_status='processing', total_amount=665.06, ingestion_time='2025-07-09T14:28:55.893919')], 
                                   'users': [Row(user_id='U000001', name=None, email='donaldgarcia@example.net', gender='Other', city='New Roberttown', signup_date=datetime.datetime(2025, 3, 13, 0, 0), ingestion_time='2025-07-07T14:28:55.893919'),
                                             Row(user_id='U000001', name='Adam Shaffer', email=None, gender='Male', city='Curtisfurt', signup_date=None, ingestion_time=None)]}


    assert deduplicates_dataframes_dict.keys() == expected_deduplicates_rows_dict.keys()
    for name in expected_deduplicates_rows_dict:
        assertDataFrameEqual(deduplicates_dataframes_dict[name], expected_deduplicates_rows_dict[name], checkRowOrder=False, rtol=0, atol=0)
    assert rejected_rows_dict.keys() == expected_rejected_rows_dict.keys()
    for name in expected_rejected_rows_dict:
        assertDataFrameEqual(rejected_rows_dict[name], expected_rejected_rows_dict[name], checkRowOrder=False, rtol=0, atol=0)




def test_deduplication_preserves_input_dictionary(sample_dataframes_with_duplicates_generator):
    inputs = sample_dataframes_with_duplicates_generator
    original_references = inputs.copy()
    result, _ = duplicates_handling(logging.getLogger("test"), inputs, duplicates_schema, "test")
    assert result is not inputs
    assert inputs.keys() == original_references.keys()
    for name, original in original_references.items():
        assert inputs[name] is original
        assert result[name] is not original



def test_deduplication_resolves_ties(spark_session):
    data = spark_session.createDataFrame(
        [("O1", 150.0, "2025-01-01"), ("O1", 100.0, "2025-01-01")],
        "order_id string, total_amount double, ingestion_time string",
    ).repartition(2)
    kept, rejected = duplicates_handling(logging.getLogger("test"), {"orders": data}, {"orders": ["order_id"]}, "test")
    assert kept["orders"].collect() == [Row(order_id="O1", total_amount=100.0, ingestion_time="2025-01-01")]
    assert rejected["orders"].collect() == [Row(order_id="O1", total_amount=150.0, ingestion_time="2025-01-01")]
