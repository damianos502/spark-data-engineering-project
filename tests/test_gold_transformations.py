import logging

import pytest
from pyspark.testing import assertDataFrameEqual
from src.transformations.gold_transformations import sales_and_revenues, geography, customer_metrics, products, orders, customers_loyalty, aggregate_revenue_per_user
from pyspark.sql.types import DayTimeIntervalType, StringType, DoubleType, StructType, StructField, IntegerType, TimestampType, FloatType, DateType, LongType, BooleanType

from src.utils.paths import TEST_EXPECTED_DATA, TEST_RAW_DATA

@pytest.fixture
def sample_orders_dataframe_loader(spark_session):
    RAW_TEST_DATA_PATH = str(TEST_RAW_DATA/"orders.csv")

    raw_schema = StructType([
                    StructField("order_id", StringType(), True),
                    StructField("user_id", StringType(), True),
                    StructField("order_date", TimestampType(), True),
                    StructField("total_amount", FloatType(), True),
                    ])

    test_orders_dataframe = spark_session.read.csv(path = RAW_TEST_DATA_PATH, 
                                                   schema = raw_schema, 
                                                   header = True)

    return test_orders_dataframe

@pytest.fixture
def sample_users_dataframe_loader(spark_session):
    RAW_TEST_DATA_PATH = str(TEST_RAW_DATA/"users.csv")

    raw_schema = StructType([
                    StructField("user_id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("gender", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("signup_date", DateType(), True)
                    ])

    test_users_dataframe = spark_session.read.csv(path = RAW_TEST_DATA_PATH, 
                                                  schema = raw_schema, 
                                                  header = True)

    return test_users_dataframe

@pytest.fixture
def sample_order_items_loader(spark_session):
    RAW_TEST_DATA_PATH = str(TEST_RAW_DATA/"order_items.csv")

    raw_schema = StructType([
                        StructField("order_item_id", StringType(), True),
                        StructField("order_id", StringType(), True),
                        StructField("product_id", StringType(), True),
                        StructField("user_id", StringType(), True),
                        StructField("quantity", IntegerType(), True),
                        StructField("item_price", DoubleType(), True),
                        StructField("item_total", DoubleType(), True)
                        ])

    test_order_items = spark_session.read.csv(path = RAW_TEST_DATA_PATH, 
                                              schema = raw_schema, 
                                              header = True)

    return test_order_items


def test_sales_and_revenues_pass(sample_orders_dataframe_loader, spark_session):
    EXPECTED_TOTAL_REVENUE = 1100.0
    EXPECTED_FINAL_PATH = str(TEST_EXPECTED_DATA/"sales_and_revenues/expected_data.csv")

    expected_final_schema = StructType([
                                StructField("order_day", DateType(), True),
                                StructField("daily_revenue", DoubleType(), True),
                                StructField("avg_total_amount", DoubleType(), True),
                                StructField("order_count", LongType(), True),
                                StructField("revenue_ranking", IntegerType(), True),
                                StructField("is_in_top_5", BooleanType(), True),
                                StructField("avg_total_amount_change", DoubleType(), True),
                                StructField("pct_change_rate", DoubleType(), True),
                            ])

    sample_orders = sample_orders_dataframe_loader

    final_dataframe, total_test_revenue = sales_and_revenues(sample_orders)
    expected_final_dataframe = spark_session.read.csv(path = EXPECTED_FINAL_PATH, 
                                                      schema = expected_final_schema, 
                                                      header = False)

    assert total_test_revenue == EXPECTED_TOTAL_REVENUE
    assertDataFrameEqual(actual = final_dataframe,
                         expected = expected_final_dataframe,
                         checkRowOrder = False,
                         rtol = 0,
                         atol = 0)


def test_geography_pass(sample_orders_dataframe_loader, sample_users_dataframe_loader, spark_session):
    EXPECTED_SHARE_IN_TOTAL_REVENUE = 50.0
    SAMPLE_TOTAL_SYSTEM_REVENUE = 420.0

    EXPECTED_FINAL_PATH = str(TEST_EXPECTED_DATA/"geography/expected_data.csv")

    logger = logging.getLogger("Test_logger")

    expected_final_schema = StructType([
                                StructField("city", StringType(), True),
                                StructField("revenue_per_city", DoubleType(), True),
                                StructField("users_count", LongType(), True),
                                StructField("avg_per_user", DoubleType(), True),
                                StructField("revenue_per_city_ranking", IntegerType(), True),
                                StructField("users_count_ranking", IntegerType(), True)
                            ])

    sample_users_rev = aggregate_revenue_per_user(sample_orders_dataframe_loader)
    sample_users = sample_users_dataframe_loader

    final_dataframe, test_share_in_total = geography(sum_per_user_dataframe = sample_users_rev, 
                                                     users_dataframe = sample_users, 
                                                     total_system_revenue = SAMPLE_TOTAL_SYSTEM_REVENUE, 
                                                     logger = logger)

    expected_final_dataframe = spark_session.read.csv(path = EXPECTED_FINAL_PATH, 
                                                      schema = expected_final_schema, 
                                                      header = False)

    assert test_share_in_total == EXPECTED_SHARE_IN_TOTAL_REVENUE
    assertDataFrameEqual(actual = final_dataframe,
                             expected = expected_final_dataframe,
                             checkRowOrder = False,
                             rtol = 0,
                             atol = 0)


def test_customer_metrics_pass(sample_orders_dataframe_loader, spark_session):
    EXPECTED_ACTIVE_USERS_COUNT = 10
    EXPECTED_AVG_ORDERS_COUNT_PER_USER = 2.0

    EXPECTED_FINAL_PATH = str(TEST_EXPECTED_DATA/"customer_metrics/expected_data.csv")

    expected_final_schema = StructType([
                                StructField("user_id", StringType(), True),
                                StructField("summed_amount", DoubleType(), True),
                                StructField("orders_count", LongType(), True),
                                StructField("spent_segment", StringType(), True),
                                StructField("more_than_1_order", BooleanType(), True)
                            ])

    sample_orders = sample_orders_dataframe_loader
    final_dataframe, active_users_count, avg_orders_count_per_user = customer_metrics(orders_dataframe = sample_orders)

    expected_final_dataframe = spark_session.read.csv(path = EXPECTED_FINAL_PATH, 
                                                      schema = expected_final_schema, 
                                                      header = False)

    assert active_users_count == EXPECTED_ACTIVE_USERS_COUNT
    assert avg_orders_count_per_user == EXPECTED_AVG_ORDERS_COUNT_PER_USER
    assertDataFrameEqual(actual = final_dataframe,
                         expected = expected_final_dataframe,
                         checkRowOrder = False,
                         rtol = 0,
                         atol = 0)


def test_products_pass(sample_order_items_loader, sample_users_dataframe_loader, spark_session):
    EXPECTED_FINAL_GOLD_PATH = str(TEST_EXPECTED_DATA/"products/gold_expected.csv")
    EXPECTED_FINAL_TOP_PRODUCTS_PATH = str(TEST_EXPECTED_DATA/"products/top_products_per_city.csv")

    expected_final_gold_schema = StructType([
                                    StructField("product_id", StringType(), True),
                                    StructField("sold_count", LongType(), True),
                                    StructField("summary_revenue", DoubleType(), True),
                                    StructField("sold_count_ranking", IntegerType(), True),
                                    StructField("top_revenue_ranking", IntegerType(), True)
                                ])

    expected_final_top_prod_schema = StructType([
                                    StructField("product_id", StringType(), True),
                                    StructField("city", StringType(), True),
                                    StructField("ordered_quantity", LongType(), True),
                                    StructField("product_per_city_ranking", IntegerType(), True)
                                ])

    sample_order_items = sample_order_items_loader
    sample_users = sample_users_dataframe_loader
    final_gold_dataframe, top_products_per_city_dataframe = products(sample_order_items, sample_users)

    expected_final_gold_dataframe = spark_session.read.csv(path = EXPECTED_FINAL_GOLD_PATH, 
                                                           schema = expected_final_gold_schema, 
                                                           header = False)
    expected_final_top_products_dataframe = spark_session.read.csv(path = EXPECTED_FINAL_TOP_PRODUCTS_PATH, 
                                                                   schema = expected_final_top_prod_schema, 
                                                                   header = False)

    assertDataFrameEqual(actual = final_gold_dataframe,
                         expected = expected_final_gold_dataframe,
                         checkRowOrder = False,
                         rtol = 0,
                         atol = 0)

    assertDataFrameEqual(actual = top_products_per_city_dataframe,
                         expected = expected_final_top_products_dataframe,
                         checkRowOrder = False,
                         rtol = 0,
                         atol = 0)


def test_orders(sample_orders_dataframe_loader, sample_order_items_loader, spark_session):
    EXPECTED_BIG_ORDERS_SHARE = 5.0

    EXPECTED_FINAL_PATH = str(TEST_EXPECTED_DATA/"orders/expected_data.csv")

    logger = logging.getLogger("Test_logger")

    expected_final_schema = StructType([
                                StructField("order_id", StringType(), True),
                                StructField("total_amount", FloatType(), True),
                                StructField("bucket", StringType(), True)
                            ])

    sample_orders = sample_orders_dataframe_loader
    sample_order_items = sample_order_items_loader

    final_dataframe, big_orders_share = orders(orders_dataframe = sample_orders, 
                                               order_items_dataframe = sample_order_items, 
                                               logger = logger)

    expected_final_dataframe = spark_session.read.csv(path = EXPECTED_FINAL_PATH, 
                                                      schema = expected_final_schema, 
                                                      header = False)

    assert big_orders_share == EXPECTED_BIG_ORDERS_SHARE
    assertDataFrameEqual(actual = final_dataframe,
                         expected = expected_final_dataframe,
                         checkRowOrder = False,
                         rtol = 0,
                         atol = 0)


def test_customers_loyalty(sample_orders_dataframe_loader, spark_session):
    EXPECTED_FINAL_PATH = str(TEST_EXPECTED_DATA/"customers_loyalty/expected_data.csv")

    expected_final_schema = StructType([
                                StructField("user_id", StringType(), True),
                                StructField("order_id", StringType(), True),
                                StructField("order_date", TimestampType(), True),
                                StructField("total_amount", FloatType(), True),
                                StructField("orders_count", LongType(), True),
                                StructField("user_total_amount", DoubleType(), True),
                                StructField("prev_order_date", TimestampType(), True),
                                StructField("orders_interval", DayTimeIntervalType(), True),
                                StructField("avg_interval", DayTimeIntervalType(), True),
                                StructField("loyalty_ranking", IntegerType(), True),
                            ])

    sample_orders = sample_orders_dataframe_loader
    final_dataframe = customers_loyalty(sample_orders)

    expected_final_dataframe = spark_session.read.csv(path = EXPECTED_FINAL_PATH, 
                                                      schema = expected_final_schema, 
                                                      header = False)

    assertDataFrameEqual(actual = final_dataframe,
                         expected = expected_final_dataframe,
                         checkRowOrder = False,
                         rtol = 0,
                         atol = 0)