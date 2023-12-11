from pyspark.sql.functions import avg, max, min, rank, desc
from pyspark.sql.window import Window

def get_stats_from_df(spark_df, groupby_col, calc_col):
    calculated_df = spark_df.groupBy(groupby_col).agg(avg(calc_col).alias(f"avg_{calc_col}"), max(calc_col).alias(f"max_{calc_col}"), min(calc_col).alias(f"min_{calc_col}"))
    return calculated_df

def get_window_ranking(spark_df, partition_col, order_cols):
    if not isinstance(order_cols, list):
        raise ValueError("order_cols needs to be type list")
    window_formula = Window.partitionBy(partition_col).orderBy([desc(col) for col in order_cols])

    return spark_df.withColumn("ranking", rank().over(window_formula))