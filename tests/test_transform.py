from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from src.jobs.etl import transform
import pytest

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('newday_test_session').getOrCreate()

def test_get_stats_from_df(spark):

    input_data = [(1, 13, 5, 978300760),
                (2, 13, 3, 978300760),
                (3, 13, 1, 978300760)]
    
    expected_output = [(13, 3, 5, 1)]

    df_input = spark.createDataFrame(data=input_data, schema=["user_id", "movie_id", "rating", "timestamp"])

    transformed_df = transform.get_stats_from_df(df_input, "movie_id", "rating")

    expected_df = spark.createDataFrame(data=expected_output, schema=["movie_id", "avg_rating", "max_rating", "min_rating"])

    assertDataFrameEqual(transformed_df, expected_df)