import configparser
from pyspark import SparkConf
import pyspark.sql.functions as f

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


def get_data(spark, db_conf):
    return spark.read \
        .format("jdbc") \
        .option("url", db_conf['url']) \
        .option("dbtable", db_conf['table']) \
        .option("user", db_conf['username']) \
        .option("password", db_conf['password']) \
        .load()


def transform_data(data_df):
    return data_df.groupBy("OrderNumber") \
                .sum("ProductPrice") \
                .select("OrderNumber", f.col("sum(ProductPrice)").alias("Total"))


def write_data(data_df, db_conf):
    return data_df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", db_conf['url']) \
        .option("dbtable", db_conf['table1']) \
        .option("truncate", "true") \
        .option("user", db_conf['username']) \
        .option("password", db_conf['password']) \
        .save()