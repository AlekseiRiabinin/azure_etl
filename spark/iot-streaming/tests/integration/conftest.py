import pytest
from tests.fixtures.test_data import create_test_stream

@pytest.fixture
def test_stream(spark):
    return create_test_stream(spark)