from pyspark.sql import SparkSession

import src.utils.logger as L
from typing import Optional
import time
from functools import reduce

from src.layers.base_layer import BasicLayer
import src.validators.schema_validator as schema_validator
import src.validators.null_validator as null_validator
import src.validators.duplicate_validator as duplicate_validator
import src.validators.business_validator as business_validator
import src.io.readers as io_readers
import src.io.writers as io_writers
import src.utils.config_loader as config_loader
from src.utils.paths import DATA_DIR, CONFIG_DIR

class SilverLayer(BasicLayer):
    """ Silver layer's class"""

    def __init__(self, 
                 spark: SparkSession
                 )  -> None:
        
        super().__init__(spark)
        self.logger = L.logging.getLogger(self.__class__.__name__)
        
    def run(self) -> Optional[bool]:
        """ Run pipeline with silver layer
        :return: True if processing finished correct."""

        silver_start_time = time.time()

        tables_names_path = CONFIG_DIR / self.input_sources["config_tables_names"]
        tables_names = config_loader.load_json(path = tables_names_path, 
                                               logger = self.logger)
        bronze_parquets_path = DATA_DIR / "bronze"

        bronze_dataframes = io_readers.read_layer_tables(spark = self.spark,
                                                         logger = self.logger,
                                                         layer_path = bronze_parquets_path,
                                                         tables_names = tables_names)

        if self.stop_if_empty(bronze_dataframes):
            return None

        cast_schema_path = CONFIG_DIR / self.input_sources["config_cast_schema"]
        cast_schema = config_loader.load_json(path = cast_schema_path, 
                                              logger = self.logger)
        casted_dataframes = schema_validator.cast_column_type(input_dataframes = bronze_dataframes,
                                                              logger = self.logger,
                                                              cast_schema = cast_schema,
                                                              cast_schema_path = self.input_sources["config_cast_schema"])

        if self.stop_if_empty(casted_dataframes):
            return None 

        columns_with_nulls = null_validator.null_search(input_dataframes = casted_dataframes,
                                                        logger = self.logger)
          
        before_denulled = casted_dataframes

        if columns_with_nulls:
            null_handling_schema_path = CONFIG_DIR / self.input_sources["config_null_handling"]
            null_handling_schema = config_loader.load_json(path = null_handling_schema_path, 
                                                           logger = self.logger)
            after_denulled = null_validator.null_handling(logger = self.logger,
                                                          input_dataframes = before_denulled,
                                                          dataframes_with_nulls = columns_with_nulls, 
                                                          null_handling_schema = null_handling_schema,
                                                          null_handling_schema_path = null_handling_schema_path)
        else:
            after_denulled = before_denulled


        duplicate_schema_path = CONFIG_DIR / self.input_sources["config_duplicate_handling"]
        duplicate_schema = config_loader.load_json(path = duplicate_schema_path, 
                                                   logger = self.logger)        
        deduplicated_dataframes, dropped_duplicates = duplicate_validator.duplicates_handling(logger = self.logger,
                                                                                              input_dataframes = after_denulled,
                                                                                              duplicate_schema = duplicate_schema,
                                                                                              duplicate_schema_path = self.input_sources["config_duplicate_handling"])

        if self.stop_if_empty(deduplicated_dataframes):
            return None
        
        business_rules_schema_path = CONFIG_DIR / self.input_sources["config_business_rules"]
        business_rules_schema = config_loader.load_json(path = business_rules_schema_path, 
                                                        logger = self.logger)
        
        enums_schema_path = CONFIG_DIR / self.input_sources["config_enums"]
        enums_schema = config_loader.load_json(path = enums_schema_path, 
                                               logger = self.logger)

        final_dataframes = business_validator.business_flagging(input_dataframes = deduplicated_dataframes, 
                                                                logger = self.logger,
                                                                spark = self.spark,
                                                                business_rules_schema = business_rules_schema,
                                                                business_rules_schema_path = self.input_sources["config_business_rules"],
                                                                enums_schema = enums_schema,
                                                                enums_schema_path = self.input_sources["config_enums"])

        if self.stop_if_empty(final_dataframes):
            return None

        io_writers.write_parquet(data = final_dataframes,
                                 logger = self.logger,
                                 layer_name = "silver")
        
        silver_end_time = time.time()
        silver_execution_time = round(silver_end_time - silver_start_time, 2)

        self.logger.info(f"Pipeline Silver succes. Total time: {silver_execution_time}")

        return True          