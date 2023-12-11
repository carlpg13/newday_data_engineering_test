from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from src.jobs.etl import transform
import pytest

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('newday_test_session').getOrCreate()

def test_get_stats_from_df(spark):

    input_data = [{"user_id":1, "movie_id":13, "rating":5, "timestamp":978300760},
                  {"user_id":2, "movie_id":13, "rating":3, "timestamp":978300760},
                  {"user_id":3, "movie_id":13, "rating":1, "timestamp":978300760}]
    
    expected_output = {"movie_id":13, "avg_rating":3, "max_rating":5, "min_rating":1}

    df_input = spark.createDataFrame(input_data)

    transformed_df = transform.get_stats_from_df(df_input, "movie_id", "rating")

    expected_df = spark.createDataFrame(expected_output)

    assertDataFrameEqual(transformed_df, expected_df)