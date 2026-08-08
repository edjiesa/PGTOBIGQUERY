import pytest
import datetime
from app.type_mapper import (
    postgres_to_bigquery_type,
    postgres_to_pyarrow_field,
    convert_value_for_pyarrow
)


def test_postgres_to_bigquery_type_basic():
    assert postgres_to_bigquery_type("integer") == ("INT64", "NULLABLE")
    assert postgres_to_bigquery_type("bigint") == ("INT64", "NULLABLE")
    assert postgres_to_bigquery_type("double precision") == ("FLOAT64", "NULLABLE")
    assert postgres_to_bigquery_type("numeric(15,2)") == ("NUMERIC", "NULLABLE")
    assert postgres_to_bigquery_type("boolean") == ("BOOL", "NULLABLE")
    assert postgres_to_bigquery_type("varchar(255)") == ("STRING", "NULLABLE")
    assert postgres_to_bigquery_type("text") == ("STRING", "NULLABLE")
    assert postgres_to_bigquery_type("jsonb") == ("JSON", "NULLABLE")
    assert postgres_to_bigquery_type("timestamp with time zone") == ("TIMESTAMP", "NULLABLE")
    assert postgres_to_bigquery_type("timestamp without time zone") == ("DATETIME", "NULLABLE")
    assert postgres_to_bigquery_type("date") == ("DATE", "NULLABLE")


def test_postgres_to_bigquery_type_array():
    assert postgres_to_bigquery_type("integer[]") == ("INT64", "REPEATED")
    assert postgres_to_bigquery_type("text[]") == ("STRING", "REPEATED")
    assert postgres_to_bigquery_type("_int4") == ("INT64", "REPEATED")


def test_convert_value_for_pyarrow():
    # JSON value conversion
    dict_val = {"key": "value", "count": 10}
    assert convert_value_for_pyarrow(dict_val, "JSON", "NULLABLE") == '{"key": "value", "count": 10}'

    # Date value
    d = datetime.date(2026, 8, 8)
    assert convert_value_for_pyarrow(d, "DATE", "NULLABLE") == d

    # Array value
    arr = ["apple", "banana"]
    assert convert_value_for_pyarrow(arr, "STRING", "REPEATED") == ["apple", "banana"]
