
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import json
import os
import pyspark
import yaml

topic = 'PinterestTopic1'
#streaming_topic = 'PinterestStreamingTopic'
bootstrap_servers = 'localhost:9092'
value_deserializer = lambda m: json.loads(m)

streaming_consumer = KafkaConsumer(topic, 
                               bootstrap_servers=bootstrap_servers, 
                               value_deserializer=value_deserializer)

# Download spark streaming package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.6.0 pyspark-shell'
spark_conf = pyspark.SparkConf().setMaster("local[*]").setAppName("spark_streaming")
# create spark session
spark = SparkSession.builder.appName("kafka_to_spark_streaming").config(conf=spark_conf).getOrCreate()
# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Define the schema for the JSON data
json_schema = StructType([
    StructField("category", StringType()),
    StructField("index", StringType()),
    StructField("unique_id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("follower_count", StringType()),
    StructField("tag_list", StringType()),
    StructField("is_image_or_video", StringType()),
    StructField("image_src", StringType()),
    StructField("downloaded", IntegerType()),
    StructField("save_location", StringType())
])

# create a streaming dataframe to receive data from Kafka
streaming_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "50") \
        .load()

# get only the pinterest post itself, which is in the "value" column of the streaming_df and cast to string
streaming_df = streaming_df.selectExpr("CAST(value as STRING)", "timestamp AS timestamp")
# turn the "value" column into a json object using defined json schema and change the name to "pinterest_posts". Only select that one column containing the pinterest posts
streaming_df = streaming_df.select(from_json(col("value"), json_schema).alias("pinterest_posts"), col("timestamp")).select("pinterest_posts.*", "timestamp")
# streaming_df = streaming_df.withColumn("timestamp", current_timestamp())
streaming_df = streaming_df.withColumn("follower_count", when(streaming_df.follower_count.endswith("k"),regexp_replace(streaming_df.follower_count,"k","000")) \
                                            .when(streaming_df.follower_count.endswith("M"),regexp_replace(streaming_df.follower_count,"M","000000")) \
                                            .when(streaming_df.follower_count == "User Info Error", None)
                                            .otherwise(streaming_df.follower_count)
                                            )
streaming_df = streaming_df.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))
streaming_df = streaming_df.withColumn("title", when(streaming_df.title == "No Title Data Available", None).otherwise(streaming_df.title))
streaming_df = streaming_df.withColumn("description", when(streaming_df.description == "No description available Story format", None).otherwise(streaming_df.description))
streaming_df = streaming_df.withColumn("image_src", when(streaming_df.image_src == "Image src error.", None).otherwise(streaming_df.image_src))
streaming_df = streaming_df.withColumn("tag_list", when(streaming_df.tag_list == "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", None).otherwise(streaming_df.tag_list))

streaming_df = streaming_df.withColumn("follower_count", streaming_df.follower_count.cast(LongType()))

streaming_df_preprocessed = streaming_df.dropna("all", subset=["title", "description", "follower_count"])

# work out number of posts made for each category in each time window
# streaming_df_preprocessed_category_count = streaming_df_preprocessed.withWatermark("timestamp", "1 minute").groupBy("category", window("timestamp", "1 minute")).count()

# work out mean number of followers per category in each time window (rounded to an integer) and then return the category that has the highest mean
streaming_df_preprocessed_mean_followers_per_category = streaming_df_preprocessed \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy("category", window("timestamp", "30 minutes")) \
    .agg(round(mean("follower_count")), max("follower_count").alias("mean_follower_count")) \
    .select("category", "mean_follower_count")

"""
# output the messages to the console 
streaming_df_preprocessed_category_count.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "true") \
    .start() \
    .awaitTermination()
"""
def write_total_num_posts_per_category_to_postgres(df):
     pass


jdbc_config = "/Users/agc/AiCore/pinterest-data-pipeline/jdbc_config.yaml"
with open(jdbc_config, "r") as f:
        jdbc_creds = yaml.safe_load(f)

jdbc_host = jdbc_creds["JDBC_HOST"]
jdbc_port = jdbc_creds["JDBC_PORT"]
jdbc_database = jdbc_creds["JDBC_DATABASE"]
jdbc_username = jdbc_creds["JDBC_USER"]
jdbc_password = jdbc_creds["JDBC_PASSWORD"]
jdbc_url = f"jdbc:postgresql://{jdbc_host}:{jdbc_port}/{jdbc_database}"


def write_to_postgres(df, batchId):
    connection_properties = {
        "user": jdbc_username,
        "password": jdbc_password,
        "driver": "org.postgresql.Driver"
    }
    df.write \
        .mode("append") \
        .jdbc(url=jdbc_url, table=jdbc_table, properties=connection_properties)


def write_pins_to_postgres(df):
    df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("truncate", "true") \
        .start() \
        .awaitTermination()


# streaming_df_preprocessed.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", "true") \
#     .start() \
#     .awaitTermination()

jdbc_table = "mean_followers_per_category" # defines table to write to in postgres db
write_pins_to_postgres(streaming_df_preprocessed_mean_followers_per_category)
