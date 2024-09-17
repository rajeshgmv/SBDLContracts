from pyspark.sql import SparkSession


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession \
                .builder \
                .master("local[3]") \
                .enableHiveSupport() \
                .getOrCreate()
    else:
        return SparkSession \
                .builder \
                .getOrCreate()
