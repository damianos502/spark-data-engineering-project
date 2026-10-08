import logging

import pytest

from src.validators.business_validator import business_flagging


@pytest.fixture
def business_tables(spark_session):
    return {
        "orders": spark_session.createDataFrame(
            [("O1", "U1", 100.0), ("O2", "missing", -5.0), ("O3", None, None)],
            "order_id string, user_id string, total_amount double",
        ),
        "users": spark_session.createDataFrame(
            [("U1", "Male")], "user_id string, gender string"
        ),
    }


def validate(spark_session, tables, rules):
    return business_flagging(
        input_dataframes=tables,
        logger=logging.getLogger("business-tests"),
        spark=spark_session,
        business_rules_schema=rules,
        business_rules_schema_path="test-rules.json",
        enums_schema={"gender": ["Male", "Female"]},
        enums_schema_path="test-enums.json",
    )



def test_business_flags_preserve_each_rule(spark_session, business_tables):
    result = validate(spark_session, business_tables, {
        "orders": {"user_id": "df_users", "total_amount": "is_positive"},
        "users": {"gender": "in_enum"},
    })
    rows = {row.order_id: row for row in result["orders"].collect()}
    assert set(rows) == {"O1", "O2", "O3"}
    assert rows["O1"].df_users_ref is True
    assert rows["O1"].correct_total_amount is True
    assert rows["O1"].business_overall_correct is True
    for order_id in ("O2", "O3"):
        assert rows[order_id].df_users_ref is False
        assert rows[order_id].correct_total_amount is False
        assert rows[order_id].business_overall_correct is False
    assert result["orders"].schema["business_overall_correct"].dataType.simpleString() == "boolean"
    assert result["users"].first().business_overall_correct is True



@pytest.mark.parametrize("rules, message", [
    ({"other": {"user_id": "is_positive"}}, "No business rules configured"),
    ({"orders": {}}, "No business rules configured"),
    ({"orders": {"missing_column": "is_positive"}}, "Missing column for validation"),
    ({"orders": {"total_amount": "unknown"}}, "Unknown rule"),
    ({"orders": {"user_id": "df_users"}}, "Cannot execute rule"),
])
def test_invalid_business_configuration(spark_session, business_tables, rules, message):
    with pytest.raises(ValueError, match=message):
        validate(spark_session, {"orders": business_tables["orders"]}, rules)
