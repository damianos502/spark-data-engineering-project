import logging
from src.utils.paths import LOGS_DIR

logging.basicConfig(filename = LOGS_DIR / "medallion.log",
                    level=logging.INFO,
                    format = 
                        ("%(asctime)s | "
                        "%(name)s | " 
                        "%(funcName)s | "
                        "%(levelname)s | "
                        "%(message)s" ),
                        force = True
                    )
