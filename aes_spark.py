from pyspark.sql import SparkSession

# Section 1: Initialize Apache Spark with the data source (Parquet)
def turbo_spark():
    spark = SparkSession \
        .builder \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.shuffle.partitions", "400") \
        .getOrCreate()
    return spark

# Section 1: Initialize Apache Spark with the data source (Parquet)
def init_spark():
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark
