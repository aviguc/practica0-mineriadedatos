import os
from pyspark.sql import SparkSession 

def get_spark_session(jar_path=None):
    builder=SparkSession.builder.appName("IBEX35") 
    if jar_path and os.path.exists(jar_path):
        builder = builder.config("spark.driver.extraClassPath", jar_path)
    return builder.getOrCreate()