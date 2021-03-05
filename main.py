from pyspark.sql import *
import pyspark.sql.functions as f

from lib.utlis import get_spark_app_config, get_db_config, get_data, write_data, transform_data

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
    orders_raw_df = get_data(spark, db_conf)

    logger.info("Repartition of Data")
    orders_df = orders_raw_df.repartition(2)

    logger.info("Transforming data")
    trans_df = transform_data(orders_df)

    logger.info("Writing result to db")
    write_data(trans_df, db_conf)

    # input("Enter\n")
    logger.info("Ending Spark Application")
    spark.stop()

if __name__ == "__main__":

    main()