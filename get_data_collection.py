import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession. \
    builder. \
    appName("proyect_name"). \
    master("local"). \
    config("spark.executor.memory", "2g"). \
    config("spark.mongodb.input.uri", "mongodb://ip:27017/name_collection"). \
    config("spark.mongodb.output.uri", "mongodb://ip:27017/name_collection"). \
    config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0"). \
    getOrCreate()


def car():
    path = "save data path"
    try:
        shutil.rmtree(path)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))

    df = spark.read.format("mongo").load()
    df = df.withColumn("data", df['data'].cast(StringType())).drop('data')
    import time
    start_time = time.time()
    df.write.save(path)
    print("--- %s seconds ---" % (time.time() - start_time))


car()
