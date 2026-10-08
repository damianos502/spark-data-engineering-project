import json
import logging
import pytest

import src.utils.preflight as preflight
from src.utils.preflight import components_check

@pytest.fixture
def component_directories(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    raw_dir = tmp_path / "raw"
    config_dir.mkdir()
    raw_dir.mkdir()
    monkeypatch.setattr(preflight, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(preflight, "RAW_DATA_DIR", raw_dir)
    return config_dir, raw_dir



def test_valid_components(tmp_path, component_directories):
    logger = logging.getLogger("test_logger")
    test_input_sources = tmp_path / "input_sources.json"

    test_sources = {
        "config_business_rules": "business_rules.json",
        "config_cast_schema": "cast_schema.json",
        "raw_events": "events.csv",
        "raw_order_items": "order_items.csv",
    }

    config_dir, raw_dir = component_directories
    for component_name, file_name in test_sources.items():
        directory = config_dir if component_name.startswith("config") else raw_dir
        (directory / file_name).touch()

    test_input_sources.write_text(
        json.dumps(test_sources),
        encoding = "utf-8"
    )

    result = components_check(logger = logger, 
                              input_sources_path = test_input_sources)
    
    assert result is True



def test_invalid_components(tmp_path, component_directories):
    logger = logging.getLogger("test_logger")
    test_input_sources = tmp_path / "input_sources.json"

    test_sources = {
        "config_business_rules": "business_rules_invalid.json",
        "config_cast_schema": "cast_schema_invalid.json",
        "raw_events": "events_invalid.csv",
        "raw_order_items": "order_items_invalid.csv",
    }

    test_input_sources.write_text(
        json.dumps(test_sources),
        encoding = "utf-8"
    )

    result = components_check(logger = logger, 
                              input_sources_path = test_input_sources)
    
    assert result is False
