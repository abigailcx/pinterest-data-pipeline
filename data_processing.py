from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import yaml

aws_credentials_file = "/Users/agc/AiCore/pinterest-data-pipeline/aws_config.yaml"
s3_pinterest_uri = "s3a://pinterest-data-40dba6ba-1777-41cd-82ea-0ca36620f0b4/posts"


def read_s3_to_spark():
    # read data from S3 bucket with Spark
    # set environment variables (looks up configs from Maven Repo)
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.430,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"
    config = SparkConf().setAppName("S3_to_Spark").setMaster("local[*]")

    sc = SparkContext(conf=config)
    spark = SparkSession(sc).builder.appName("S3_to_Spark").master("local[*]").getOrCreate()

    with open(aws_credentials_file, "r") as f:
        aws_creds = yaml.safe_load(f)
    
    # set up hadoop configuration
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", aws_creds["AWS_ACCESS_KEY"])
    hadoopConf.set("fs.s3a.secret.key", aws_creds["AWS_SECRET_KEY"])
    hadoopConf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    # create spark dataframe to read in files from the S3 bucket
    df = spark.read.json(s3_pinterest_uri)
    # df.show(20)
    
    return df

def preprocess_data(df):
    df = df.withColumn("follower_count", when(df.follower_count.endswith("k"),regexp_replace(df.follower_count,"k","000")) \
                                            .when(df.follower_count.endswith("M"),regexp_replace(df.follower_count,"M","000000")) \
                                            .when(df.follower_count == "User Info Error", None)
                                            .otherwise(df.follower_count)
                                            )
    df = df.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))
    df = df.withColumn("title", when(df.title == "No Title Data Available", None).otherwise(df.title))
    df = df.withColumn("description", when(df.description == "No description available Story format", None).otherwise(df.description))
    df = df.withColumn("image_src", when(df.image_src == "Image src error.", None).otherwise(df.image_src))
    df = df.withColumn("tag_list", when(df.tag_list == "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", None).otherwise(df.tag_list))
    
    df = df.withColumn("follower_count", df.follower_count.cast(LongType()))
    
    df_preprocessed = df.dropna("all", subset=["title", "description", "follower_count"])
    
    df_preprocessed.where(col("tag_list").isNull()).show()
    df_preprocessed.printSchema()
    

    return df_preprocessed


if __name__ == "__main__":
    preprocess_data(read_s3_to_spark())

