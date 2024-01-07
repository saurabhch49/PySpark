from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkApp") \
        .getOrCreate()

    logger = Log4j(spark)

    flightDf = spark.read \
                .format("parquet") \
                .load("DataSource/flight-time.parquet")

    flightDf.show()

    # get number of partion by converting dataFrame to RDD and use it's method
    print("Before Repartition",flightDf.rdd.getNumPartitions(),sep=" : ")

    # this is an iinbuild function need to be imported. return the partion id of the dataFrame. Below is checking
    # that how many record contains each partition
    flightDf.groupBy(spark_partition_id()).count().show()

    """" Note : If executor are 3 but the partition are 5, then 
        Executor 1 : work on 2 partition
        Executor 2 : work on 2 partition
        Executor 3 : work on 1 partition 
    """
    partitionDF = flightDf.repartition(5)
    print("After Repartition",partitionDF.rdd.getNumPartitions(),sep=" : ")
    partitionDF.groupby(spark_partition_id()).count().show()

    partitionDF.write \
                .format("json") \
                .mode("overwrite") \
                .option("path", "DataSink/json1/") \
                .save()

    flightDf.write \
            .format("json") \
            .mode("overwrite") \
            .option("path", "DataSink/json2/") \
            .partitionBy("OP_CARRIER", "ORIGIN") \
            .option("maxRecordsPerFile", "400000") \
            .save()





