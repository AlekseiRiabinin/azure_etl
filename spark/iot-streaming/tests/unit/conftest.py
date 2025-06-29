import pytest
from tests.fixtures.test_data import get_spark_session

@pytest.fixture(scope="module")
def spark():
    spark = get_spark_session()
    yield spark
    spark.stop()
