import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("pipeline_tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield session
    session.stop()
