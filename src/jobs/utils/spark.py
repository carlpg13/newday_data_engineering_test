from pyspark.sql import SparkSession


def start_spark(app_name="my_spark_app", master="local[*]"):
    """Start Spark session and get Spark logger.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    # get Spark session factory
    spark_builder = (
        SparkSession.builder.master(master)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "logs")
        .appName(app_name)
    )

    # create session
    spark_session = spark_builder.getOrCreate()

    return spark_session
