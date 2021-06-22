from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/Users/alejandro/Downloads/postgresql-42.2.18.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/db_name") \
    .option("dbtable", "otp") \
    .option("user", "postgres") \
    .option("password", "1602") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
