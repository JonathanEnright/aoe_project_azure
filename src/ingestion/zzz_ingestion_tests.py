import io
from datetime import date

import pandas as pd
import pytest
from pydantic import BaseModel

from src.common.extract_utils import (
    create_stats_endpoints,
    generate_weekly_queries,
    validate_json_schema,
    validate_parquet_schema,
)


# Test Data and Fixtures
@pytest.fixture
def sample_dates():
    return {"start_date": date(2023, 1, 1), "end_date": date(2023, 1, 14)}


@pytest.fixture
def sample_endpoints():
    return [{"dated": date(2023, 1, 1), "query_str": "2023-01-01_2023-01-07"}]


@pytest.fixture
def sample_validation_schema():
    class TestSchema(BaseModel):
        id: int
        name: str

    return TestSchema


# Tests for validate_parquet_schema
def test_validate_parquet_schema(sample_validation_schema):
    # Create a sample DataFrame
    df = pd.DataFrame([{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}])
    cast_mapping = {"id": "int64"}
    # Create a parquet file in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer)
    parquet_buffer.seek(0)

    result = validate_parquet_schema(parquet_buffer, sample_validation_schema, cast_mapping)
    assert result is not None


# Tests for validate_json_schema
def test_validate_json_schema_valid(sample_validation_schema):
    valid_data = {"id": 1, "name": "test"}
    result = validate_json_schema(valid_data, sample_validation_schema)
    assert result.id == 1
    assert result.name == "test"


def test_validate_json_schema_invalid(sample_validation_schema):
    invalid_data = {"id": "not_an_integer", "name": "test"}
    result = validate_json_schema(invalid_data, sample_validation_schema)
    assert result == []


# Tests for 'stats' ingestion
def test_generate_weekly_queries(sample_dates):
    queries = generate_weekly_queries(
        sample_dates["start_date"], sample_dates["end_date"]
    )
    assert len(queries) == 2  # Should generate 2 weeks worth of queries
    assert all("dated" in q and "query_str" in q for q in queries)
    assert queries[0]["query_str"].startswith("2023-01-01")


def test_create_stats_endpoints(sample_endpoints):
    extract_file = "stats"
    endpoints = create_stats_endpoints(extract_file, sample_endpoints)
    assert len(endpoints) == 1
    assert endpoints[0]["file_date"] == "2023-01-01"
    assert endpoints[0]["endpoint_str"] == "2023-01-01_2023-01-07/stats.parquet"
