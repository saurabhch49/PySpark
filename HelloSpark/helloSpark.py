from pyspark.sql import *

if __name__ == "__main__" :
    spark = (SparkSession.builder \
        .appName("Hello Spark")  \
        .master("local[3]") \
        # using local JVM cluter manager with 3 threads
        .getOrCreate())

    spark.stop()
