from pyspark.sql import *
import pyspark.sql.functions as f

from lib.utlis import get_spark_app_config, get_db_config
from lib.logger import Log4j

def main():

    spark_conf =get_spark_app_config()
    spark =SparkSession.builder \
            .config(conf=spark_conf) \
            .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting Spark Application")

    logger.info("Readind DB configurations")
    db_conf = get_db_config()

    logger.info("Reading data from database")
    orders_raw_df = spark.read \
        .format("jdbc") \
        .option("url", db_conf['url']) \
        .option("dbtable", db_conf['table']) \
        .option("user", db_conf['username']) \
        .option("password", db_conf['password']) \
        .load()

    logger.info("Repartition of Data")
    orders_df = orders_raw_df.repartition(2)

    logger.info("Transforming data")
    trans_df = orders_df.groupBy("OrderNumber") \
                .sum("ProductPrice") \
                .select("OrderNumber", f.col("sum(ProductPrice)").alias("Total"))

    logger.info("Writing result to db")
    trans_df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", db_conf['url']) \
        .option("dbtable", db_conf['table1']) \
        .option("truncate", "true") \
        .option("user", db_conf['username']) \
        .option("password", db_conf['password']) \
        .save()

    input("Enter\n")
    logger.info("Ending Spark Application")
    spark.stop()

if __name__ == "__main__":

    main()