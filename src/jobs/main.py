from utils.spark import start_spark
from etl.extract import read_file
from etl.transform import get_stats_from_df, get_window_ranking
import os

def main():
        # start Spark application and get Spark session, logger and config
    spark_session = start_spark(
        app_name='new_day_test')
    
    df_movies, df_ratings = _read_movies_rating_files(spark_session)

    df_movies_ratings = _transform_rating_movies_df(df_movies, df_ratings)

    df_top3_movies = _get_df_top_3_rating_by_user(df_ratings)

    _write_parquet_files(df_movies, "original_movies")
    _write_parquet_files(df_ratings, "original_ratings")
    _write_parquet_files(df_movies_ratings, "movies_with_ratings")
    _write_parquet_files(df_top3_movies, "top3_movies_by_user")

    spark_session.stop()


def _read_movies_rating_files(spark_session):
    df_movies = read_file(spark_session, file_name="movies.dat", cols_name=["movie_id", "title", "genre"])
    df_ratings = read_file(spark_session, file_name="ratings.dat", cols_name=["user_id", "movie_id", "rating", "timestamp"])
    return df_movies, df_ratings

def _transform_rating_movies_df(df_movies, df_ratings):
    df_movies_ratings = get_stats_from_df(df_ratings, "movie_id", "rating")
    return df_movies.join(df_movies_ratings, "movie_id", "inner")

def _get_df_top_3_rating_by_user(df_ratings):
    df_ranking = get_window_ranking(df_ratings, partition_col="user_id", order_cols=["rating", "movie_id", "timestamp"])
    df_ranking.where("ranking <= 3")

def _write_parquet_files(df_spark, file_name):
    full_file_name = os.path.join(os.getcwd(), "volumes", f"{file_name}.parquet")
    df_spark.write.parquet(full_file_name, mode="overwrite")

if __name__ == "__main__":
    print("I'm running")
    main()
