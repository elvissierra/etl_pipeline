import pandas as pd
import pytest

from transform_data_module import transform_data, validate_schema


def test_validate_schema_pass():
    df = pd.DataFrame({
        "place_id": [1],
        "edited_fields": ["field"],
        "popularity": [10]
    })
    validate_schema(df, ["place_id", "edited_fields", "popularity"])


def test_validate_schema_fail():
    df = pd.DataFrame({"place_id": [1]})
    with pytest.raises(ValueError):
        validate_schema(df, ["place_id", "popularity"])


def test_transform_data():
    df = pd.DataFrame({
        "place_id": [1, 2],
        "edited_fields": ["a", None],
        "all_customer_suggested_fields_edited": ["No", "Yes"],
        "popularity": [100, 200]
    })
    result = transform_data(
        df,
        required_cols=["place_id", "edited_fields", "popularity"],
        filter_col="all_customer_suggested_fields_edited",
        filter_value="Yes"
    )
    assert len(result) == 1
    assert result.iloc[0]["place_id"] == 1