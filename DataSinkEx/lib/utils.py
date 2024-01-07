import configparser

from pyspark import SparkConf

#This function reads all the configs from the "Spark.conf" file, set them to the SparkConf object, and return the ready to use SparkConf.

#Now, we need to come back to the main and use it in the spark builder dot config method.
# https://www.youtube.com/watch?v=fHpZOgbe_J8
def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf
