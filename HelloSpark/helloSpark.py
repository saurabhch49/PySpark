from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting  HelloSpark")
    # Your processing code
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    logger.info("Finished  HelloSpark")
    spark.stop()


