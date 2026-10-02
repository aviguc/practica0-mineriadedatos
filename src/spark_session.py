import os
from pyspark.sql import SparkSession 

def get_spark_session(jar_path=None):
    directorio_src = os.path.dirname(os.path.abspath(__file__))
    
    if jar_path is None:
        for archivo in os.listdir(directorio_src):
            if archivo.endswith(".jar") and "mysql-connector" in archivo:
                jar_path = os.path.join(directorio_src, archivo)
                break

    builder = SparkSession.builder.appName("IBEX35")
    
    if jar_path and os.path.exists(jar_path):
        builder = (
            builder
            .config("spark.driver.extraClassPath", jar_path)
            .config("spark.executor.extraClassPath", jar_path)
        )
        
    return builder.getOrCreate()


