from logging import Logger
import src.utils.config_loader as config_loader
from pathlib import Path
from src.utils.paths import CONFIG_DIR, RAW_DATA_DIR


def components_check(logger: Logger,
                     input_sources_path: Path
                     ) -> bool:

    paths = config_loader.load_json(path = input_sources_path, 
                                    logger = logger)
    components_valid = True


    if paths is not None:
        for component_name, file_name in paths.items():
            if component_name.startswith("config"):
                full_path = CONFIG_DIR / file_name
            elif component_name.startswith("raw"):
                full_path = RAW_DATA_DIR / file_name

            if not full_path.exists():
                logger.error(f"Missing component: {full_path}")
                components_valid = False

    return components_valid