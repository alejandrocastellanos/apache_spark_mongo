import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, lit

# Create Spark session
spark = SparkSession.builder \
    .appName("proyect_name") \
    .master("local") \
    .getOrCreate()


async def report_clients(data: dict):
    start_time = time.time()

    #read data
    loan_df = spark.read.option("multiline", "true").\
        json("/Users/alejandro/Documents/Courses/Apache Spark/json_files/loan.json")
    person_df = spark.read.option("multiline", "true").\
        json("/Users/alejandro/Documents/Courses/Apache Spark/json_files/person.json")

    #select columns
    new_loan_df = loan_df.select("_id")
    new_person_df = person_df.select("identification")

    #join dataframes
    ta = new_loan_df.alias('ta')
    tb = new_person_df.alias('tb')
    inner_join = tb.join(ta, tb.identification == ta.debtor_identification)

    #select data after join
    loan_clients = inner_join.select("_id")
    #format field column
    df = loan_clients.withColumn("datetype_timestamp", to_timestamp(col("created")))

    #ending query
    filter_result = df.filter(df["datetype_timestamp"] >= lit('2020-07-01')).filter(df["datetype_timestamp"] <=
                                                                                    lit('2020-09-30'))

    #export csv
    filter_result.write.csv('/Users/alejandro/Desktop/mycsv4.csv')


    print("--- %s seconds ---" % (time.time() - start_time))
