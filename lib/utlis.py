import configparser
from pyspark import SparkConf


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def get_db_config():
    config = configparser.ConfigParser()
    config.read("conf/db.conf")
    return config['postgresql']


def get_data():
    return