from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
    spark = SparkSession \
        .builder \
        .appName("PythonStreamingKafkaTweetCount") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    #Preparing schema for tweets
    schema = StructType([
    	StructField("text", StringType()),
        StructField("user", StructType([
            StructField("id",StringType()),
            StructField("name",StringType())
        ])),
    ])
    #Read from kafka topic named "twitter"
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter8") \
        .option("startingOffsets", "earliest") \
        .load()

    #kafka_df.printSchema()

    value_df = kafka_df.select(from_json(col("value").cast("string"),schema).alias("value"))

    #value_df.printSchema()

    explode_df = value_df.selectExpr("value.text",
                                     "value.user.id", "value.user.name")




    kafka_target_df = explode_df.selectExpr("id as key",
                                                 "to_json(struct(*)) as value")
    kafka_target_df.printSchema()

    nifi_query = kafka_target_df \
            .writeStream \
            .queryName("Notification Writer") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "TwitterConsume") \
            .outputMode("append") \
            .option("checkpointLocation", "chk-point-dir") \
            .start()

    nifi_query.awaitTermination()
