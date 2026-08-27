from pyspark.sql import SparkSession

import src.utils.logger as L
import time
from typing import Optional

from src.layers.base_layer import BasicLayer

import src.validators.schema_validator as schema_validator
import src.validators.content_validator as content_validator
import src.transformations.bronze_transformations as bronze_transformations
import src.io.readers as io_readers
import src.io.writers as io_writers
import src.utils.config_loader as config_loader
from src.utils.paths import CONFIG_DIR, RAW_DATA_DIR

class BronzeLayer(BasicLayer):
    """ Bronze layer's class"""

    def __init__(self,
                 spark: SparkSession,
                 skip_invalid_tables: bool = False
                 ) -> None:                                                            

        super().__init__(spark)

        self.skip_invalid_tables = skip_invalid_tables
        self.logger = L.logging.getLogger(self.__class__.__name__)
         
    def run(self) -> Optional[bool]:
        """ Run pipeline with bronze layer
        :return: True if processing finished correct."""
        
        bronze_start_time = time.time()
        files_names = []

        tables_names_path = CONFIG_DIR / self.input_sources["config_tables_names"]
        tables_names = config_loader.load_json(path = tables_names_path, 
                                               logger = self.logger)

        for name in tables_names:
            csv_name = name + ".csv"
            files_names.append(csv_name)

        if self.stop_if_empty(files_names):
            return None

        raw_path = RAW_DATA_DIR
        raw_dataframes = io_readers.read_raw_data(expected_file_names = files_names, 
                                                  spark = self.spark, 
                                                  logger = self.logger, 
                                                  raw_folder_path = raw_path, 
                                                  skip_invalid_tables = self.skip_invalid_tables)

        if self.stop_if_empty(raw_dataframes):
            return None

        expected_columns_path = CONFIG_DIR / self.input_sources["config_raw_expected_columns"]
        expected_columns_dict = config_loader.load_json(path = expected_columns_path, 
                                                        logger = self.logger)
        valid_col_dataframes = schema_validator.columns_validation(raw_dataframes = raw_dataframes,
                                                                   logger = self.logger,
                                                                   expected_columns_dict = expected_columns_dict,
                                                                   skip_invalid_tables = self.skip_invalid_tables)

        if self.stop_if_empty(valid_col_dataframes):
            return None

        empty_checked_dataframes = content_validator.empty_content_check(dataframes = valid_col_dataframes,
                                                                         logger = self.logger,
                                                                         skip_invalid_tables = self.skip_invalid_tables)

        if self.stop_if_empty(empty_checked_dataframes):
            return None

        final_dataframes = bronze_transformations.add_ingestion_columns(dataframes = empty_checked_dataframes,
                                                                        logger = self.logger)

        if self.stop_if_empty(final_dataframes):
            return None
        
        io_writers.write_parquet(data = final_dataframes,
                                 logger = self.logger,
                                 layer_name = "bronze")

        bronze_end_time = time.time()
        bronze_execution_time = round(bronze_end_time - bronze_start_time, 2)

        self.logger.info(f"Pipeline Bronze success. Total time: {bronze_execution_time}")
        return True       