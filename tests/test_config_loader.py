import pytest
import json
import logging

from src.utils.config_loader import load_json


def test_load_json_returns_dictionary(tmp_path):
    # arrange
    test_file = tmp_path / "test_config.json"
    test_data = {
        "orders": "data/raw/orders.csv",
        "users": "data/raw/users.csv"
    }

    test_file.write_text(
        json.dumps(test_data),
        encoding = "utf-8"
    )

    logger = logging.getLogger("test_logger")

    # act
    result = load_json(
        path = test_file,
        logger = logger
    )

    # assert
    assert result == test_data


def test_missing_config_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_json(tmp_path / "missing.json", logging.getLogger("test"))



def test_invalid_json_raises(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text('{broken json', encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        load_json(path, logging.getLogger("test"))
