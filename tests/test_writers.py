import json
import logging

import pytest
from pyspark.testing import assertDataFrameEqual

import src.io.writers as writers


@pytest.fixture
def report_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(writers, "ADDITIONAL_REPORTS", tmp_path)
    return tmp_path


def test_json_report_creation_and_history(report_directory):
    logger = logging.getLogger("writer-tests")
    first = {"active_users_count": 10, "avg_orders": None}
    second = {"active_users_count": 20, "avg_orders": 2.5}
    writers.write_json_report(first, "report", logger)
    path = report_directory / "report.json"
    assert json.loads(path.read_text()) == [first]
    writers.write_json_report(second, "report", logger)
    assert json.loads(path.read_text()) == [first, second]


def test_json_report_truncates_old_content(report_directory):
    path = report_directory / "report.json"
    path.write_text('[{"active_users_count": 10}]' + ' ' * 1000)
    new_report = {"active_users_count": 20}
    writers.write_json_report(new_report, "report", logging.getLogger("writer-tests"))
    expected = [{"active_users_count": 10}, new_report]
    assert path.read_text() == json.dumps(expected, indent=4)


def test_parquet_report_overwrites_previous_result(spark_session, report_directory):
    logger = logging.getLogger("writer-tests")
    first = spark_session.createDataFrame([("P1", 5), ("P2", 2)], "product_id string, quantity long")
    second = spark_session.createDataFrame([("P3", 7)], first.schema)
    writers.write_parquet_report(first, logger, "products")
    assertDataFrameEqual(spark_session.read.parquet(str(report_directory / "products")), first)
    writers.write_parquet_report(second, logger, "products")
    assertDataFrameEqual(spark_session.read.parquet(str(report_directory / "products")), second)
