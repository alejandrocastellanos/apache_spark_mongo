from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/Users/alejandro/Downloads/mongo-java-driver-3.12.6.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mongodb:Server=127.0.0.1;Port=27017;Database=test;") \
    .option("dbtable", "user") \
    .option("driver", "cdata.jdbc.mongodb.MongoDBDriver") \
    .load()

df.printSchema()
from pyspark.sql.functions import desc
df.limit(10)
df.printSchema()
df.sort(desc("created"))
results = df.toJSON().map(lambda j: json.loads(j)).collect()
df1 = df.withColumn('created', col("created").cast(DateType()))
df1 = df1.filter(df1["created"] >= '2020-06-01').cache()
df1.write.save("/Users/alejandro/Documents/Courses/Apache Spark/supertable.parquet")
df1.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save()

df4.write.csv('/Users/alejandro/Documents/Courses/Apache Spark/mycsv.csv')


df4.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('path')
df2 = spark.read.option("header", True) .csv("test.csv/")
