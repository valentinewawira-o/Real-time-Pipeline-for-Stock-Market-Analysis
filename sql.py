#sep-up spark Streming

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StructType,StructField,StringType,FloatType,LongType

spark = SparkSession.builder.appName('StockMarketAnalysis').getOrCreate()

schema = StructType([
    StructField('timestamp', LongType(), True),
    StructField('price', FloatType(), True),
    StructField('symbol', StringType(), True)

])

df= spark \
    .readstram\
        .format("kafka") \
            .option("kafka.bootstrap.servers", "pkc-12576z.us-west2.gcp.confluent.cloud:9092") \
                .option("subscribe", "stock-market-data") \
                    .option("kafka.security.protocol", "SASL_SSL") \
                        .option("kafka.sasl.mechanism", "PLAIN") \
                            .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='FS2XJUVQM72JZRS3' password='wToHN73jRLbvDHIs4EzD4+O4u4IZ8vbrZQZehQ05EOqKagjgjx+GaglB6A5K5nA3';") \
                                .load()

stock_data = df.selectExpr("CAST(value AS STRING) as json").select(from_json(col("json"), schema).alias("data")).select("data.*")

query = stock_data \
    .writeStream \
    .format("console") \
    .start()

query.awaitTermination()

from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.stock_market
collection = db.stock_data

def save_to_mongodb(data):
    collection.insert_one(data)

if __name__ == '__main__':
    for record in stock_data:
        save_to_mongodb(record)