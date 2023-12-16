import sys

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

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting  HelloSpark")
    # Your processing code
    #conf_out = spark.sparkContext.getConf()
    #logger.info(conf_out.toDebugString())
    #logger.info("Finished  HelloSpark")

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[1])
    # set python parameter
    survey_df.show()
    spark.stop()


