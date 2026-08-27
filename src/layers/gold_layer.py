from pyspark.sql import SparkSession

import src.utils.logger as L
from typing import Optional
import time

from src.layers.base_layer import BasicLayer
import src.validators.schema_validator as schema_validator
import src.transformations.gold_transformations as gold_transformations
import src.io.readers as io_readers
import src.io.writers as io_writers
import src.utils.config_loader as config_loader
from src.utils.paths import CONFIG_DIR, DATA_DIR

class GoldLayer(BasicLayer):
    """ Gold layer's class"""


    def __init__(self, spark: SparkSession) -> None:
        
        super().__init__(spark)
        self.logger = L.logging.getLogger(self.__class__.__name__)
        

    def run(self) -> Optional[bool]:
        """ Run pipeline with gold layer
        :return: True if processing finished correct."""

        gold_start_time = time.time()

        tables_names_path = CONFIG_DIR / self.input_sources["config_tables_names"]
        tables_names = config_loader.load_json(path = tables_names_path, 
                                               logger = self.logger)
        layer_parquet_path = DATA_DIR / "silver"

        silver_dataframes = io_readers.read_layer_tables(spark = self.spark,
                                                         logger = self.logger,
                                                         layer_path = layer_parquet_path,
                                                         tables_names = tables_names)

        if self.stop_if_empty(silver_dataframes):
            return None

        schema_path = CONFIG_DIR / self.input_sources["config_gold_expected_schema"]
        schema = config_loader.load_json(path = schema_path, 
                                         logger = self.logger)

        if not schema:
            return None

        schema_correct = schema_validator.check_dataframes_schema(silver_dataframes = silver_dataframes,
                                                                  expected_schema = schema,
                                                                  logger = self.logger)
        
        if not schema_correct:
            return None
        
        orders_dataframe = silver_dataframes["orders"]
        users_dataframe = silver_dataframes["users"]
        order_items_dataframe = silver_dataframes["order_items"]

        gold_sales_and_revenues, total_system_revenue = gold_transformations.sales_and_revenues(orders_dataframe = orders_dataframe)        
        
        io_writers.write_parquet(data = gold_sales_and_revenues,
                                 logger = self.logger,
                                 layer_name = "gold",
                                 transformation_name = "sales_and_revenues")


        gold_geography, share_top_users_in_total_revenue = gold_transformations.geography(orders_dataframe = orders_dataframe, 
                                                                                          users_dataframe = users_dataframe, 
                                                                                          total_system_revenue = total_system_revenue,
                                                                                          logger = self.logger)
        io_writers.write_parquet(data = gold_geography,
                                 logger = self.logger,
                                 layer_name = "gold",
                                 transformation_name = "geography")


        gold_customer_metrics, active_users_count, avg_orders_count_per_user = gold_transformations.customer_metrics(orders_dataframe = orders_dataframe)
        io_writers.write_parquet(data = gold_customer_metrics,
                                 logger = self.logger,
                                 layer_name = "gold",
                                 transformation_name = "customer_metrics")


        gold_products, top_products_per_city = gold_transformations.products(order_items_dataframe = order_items_dataframe, 
                                                                             users_dataframe = users_dataframe) 
        io_writers.write_parquet(data = gold_products,
                                 logger = self.logger,
                                 layer_name = "gold",
                                 transformation_name = "products")


        gold_orders, big_orders_share = gold_transformations.orders(orders_dataframe = orders_dataframe, 
                                                                    order_items_dataframe = order_items_dataframe,
                                                                    logger = self.logger) 
        io_writers.write_parquet(data = gold_orders,
                                 logger = self.logger,
                                 layer_name = "gold",
                                 transformation_name = "orders")


        gold_customers_loyalty = gold_transformations.customers_loyalty(orders_dataframe = orders_dataframe)
        io_writers.write_parquet(data = gold_customers_loyalty,
                                 logger = self.logger,
                                 layer_name = "gold",
                                 transformation_name = "customers_loyalty")


        gold_end_time = time.time()
        gold_execution_time = round(gold_end_time - gold_start_time, 2)

        self.logger.info(f"Pipeline Gold success. Total time: {gold_execution_time}")

        return True    