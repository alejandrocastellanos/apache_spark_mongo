import os

from pyspark.sql import SparkSession
from schemas import *


class SparkSessions:

    def __init__(self):
        self.environment = os.environ["APP_ENVIRONMENT"]
        self.host = os.environ["MONGO_HOST"]
        self.port = os.environ["MONGO_PORT"]
        self.data_base = f"mongodb://{self.host}:{self.port}/{self.environment}_supertable_db"

    def create_session(self):
        spark_session = SparkSession.builder.appName("supertable-spark").\
            master("local[4]").config("spark.executor.memory", "4g").config("spark.driver.memory", "15g").\
            config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").getOrCreate()
        spark_session.conf.set("spark.sql.inMemoryColumnarStorage.compressed", True)
        spark_session.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 10000)
        return spark_session

    def collection_name(self):
        collection = f"{self.data_base}.collection_name"
        return self.create_session().read.format("mongo").option("uri", collection).schema(schema_supertable()).load()


    def stop_session(self):
        return self.create_session().stop()
