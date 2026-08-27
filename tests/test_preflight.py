import json
import logging

from src.utils.preflight import components_check

def test_valid_components(tmp_path):
    logger = logging.getLogger("test_logger")
    test_input_sources = tmp_path / "input_sources.json"

    test_sources = {
        "config_business_rules": "business_rules.json",
        "config_cast_schema": "cast_schema.json",
        "raw_events": "events.csv",
        "raw_order_items": "order_items.csv",
    }

    test_input_sources.write_text(
        json.dumps(test_sources),
        encoding = "utf-8"
    )

    result = components_check(logger = logger, 
                              input_sources_path = test_input_sources)
    
    assert result == True


def test_invalid_components(tmp_path):
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
    
    assert result == False