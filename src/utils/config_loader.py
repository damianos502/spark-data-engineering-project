import json
from logging import Logger
from pathlib import Path

def load_json(path: str | Path, 
              logger: Logger
              ) -> dict | list:
    """ Read .json file
    :param path: File path
    :param logger: Standard Logger
    :return: File's content"""

    try:
        with open(path) as json_data:
            data = json.load(json_data)
            return data
    except OSError as e:
        logger.exception(f"Source file opening error: {path}")
        raise