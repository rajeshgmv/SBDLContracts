import configparser
from pyspark import SparkConf

def get_config(env):
    config = configparser.ConfigParser()
    config.read("conf\sbdl.conf")
    conf = {}
    for (key,value) in config.items(env):
        conf[key] = value
    return conf

def get_spark_conf(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    for (key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf
