from pyspark.sql import SparkSession
import src.utils.logger as L

import src.utils.config_loader as config_loader
from src.utils.paths import INPUT_SOURCES_PATH

class BasicLayer:
    """ Basic class for pipeline's layers"""
    
    def __init__(self, spark: SparkSession | None = None) -> None:
        self.spark = spark
        self.logger = L.logging.getLogger(self.__class__.__name__)
        self.input_sources = config_loader.load_json(path = INPUT_SOURCES_PATH, 
                                                     logger = self.logger)           
            
    def stop_if_empty(self, dataframes):
        """ Stopping pipeline if no DataFrames"""

        if not dataframes:
            self.logger.warning(f"Pipeline {self.__class__.__name__} aborted.")
            return True
        return False    
