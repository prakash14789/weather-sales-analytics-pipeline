import pytest
import pandas as pd
import numpy as np
from src.validation import DatasetValidator

@pytest.fixture
def sample_dataframe():
    """Generates a sample DataFrame for testing validation rules."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "David"],
        "age": [25, 30, 35, 40],
        "score": [95.5, 80.0, 75.25, 90.0],
        "category": ["A", "B", "A", "C"],
        "order_date": pd.to_datetime(["2014-05-10", "2015-06-15", "2016-07-20", "2017-08-25"]),
        "has_nulls": [1.0, 2.0, np.nan, 4.0]
    })

def test_expect_table_columns_to_match_set(sample_dataframe):
    validator = DatasetValidator(sample_dataframe, "Test Dataset")
    
    # Success cases
    assert validator.expect_table_columns_to_match_set(["id", "name", "age"])
    assert validator.expect_table_columns_to_match_set(["id", "name", "age", "score", "category", "order_date", "has_nulls"])
    
    # Failure cases
    assert not validator.expect_table_columns_to_match_set(["id", "missing_col"])
    assert not validator.expect_table_columns_to_match_set(["id", "name"], ordered=True)

def test_expect_column_to_exist(sample_dataframe):
    validator = DatasetValidator(sample_dataframe, "Test Dataset")
    assert validator.expect_column_to_exist("id")
    assert not validator.expect_column_to_exist("missing_col")

def test_expect_column_values_to_not_be_null(sample_dataframe):
    validator = DatasetValidator(sample_dataframe, "Test Dataset")
    assert validator.expect_column_values_to_not_be_null("id")
    assert not validator.expect_column_values_to_not_be_null("has_nulls")
    assert not validator.expect_column_values_to_not_be_null("missing_col")

def test_expect_column_values_to_be_of_type(sample_dataframe):
    validator = DatasetValidator(sample_dataframe, "Test Dataset")
    
    assert validator.expect_column_values_to_be_of_type("id", "integer")
    assert validator.expect_column_values_to_be_of_type("score", "numeric")
    assert validator.expect_column_values_to_be_of_type("name", "string")
    assert validator.expect_column_values_to_be_of_type("order_date", "datetime")
    
    # Failure case
    assert not validator.expect_column_values_to_be_of_type("name", "integer")

def test_expect_column_values_to_be_between(sample_dataframe):
    validator = DatasetValidator(sample_dataframe, "Test Dataset")
    
    assert validator.expect_column_values_to_be_between("age", 20, 50)
    assert validator.expect_column_values_to_be_between("score", 70.0, 100.0)
    assert validator.expect_column_values_to_be_between("order_date", "2014-01-01", "2018-01-01", parse_date=True)
    
    # Failure case
    assert not validator.expect_column_values_to_be_between("age", 30, 50) # Alice is 25

def test_expect_column_values_to_be_in_set(sample_dataframe):
    validator = DatasetValidator(sample_dataframe, "Test Dataset")
    
    assert validator.expect_column_values_to_be_in_set("category", ["A", "B", "C", "D"])
    
    # Failure case
    assert not validator.expect_column_values_to_be_in_set("category", ["A", "B"]) # David is category C

def test_expect_column_values_to_be_greater_than_or_equal_to(sample_dataframe):
    validator = DatasetValidator(sample_dataframe, "Test Dataset")
    
    assert validator.expect_column_values_to_be_greater_than_or_equal_to("age", 25)
    
    # Failure case
    assert not validator.expect_column_values_to_be_greater_than_or_equal_to("age", 30) # Alice is 25

def test_expect_table_row_count_to_be_between(sample_dataframe):
    validator = DatasetValidator(sample_dataframe, "Test Dataset")
    
    assert validator.expect_table_row_count_to_be_between(2, 6)
    
    # Failure case
    assert not validator.expect_table_row_count_to_be_between(5, 10) # count is 4
