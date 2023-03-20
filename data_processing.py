from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import pyspark
import yaml

aws_credentials_file = "aws_config.yaml"

# read data from S3 bucket with Spark
# set environment variables (looks up configs from Maven Repo)
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.430,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"
config = SparkConf().setAppName("S3_to_Spark")

sc = SparkContext(conf=config)
spark = SparkSession(sc).builder.appName("S3_to_Spark").getOrCreate()

with open(aws_credentials_file, "r") as f:
    aws_creds = yaml.safe_load(f)
# set up hadoop configuration
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", aws_creds["AWS_ACCESS_KEY"])
hadoopConf.set("fs.s3a.secret.key", aws_creds["AWS_SECRET_KEY"])
hadoopConf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# create spark dataframe to read in files from the S3 bucket
while True:
    df = spark.read.json(f"s3a://pinterest-data-40dba6ba-1777-41cd-82ea-0ca36620f0b4/pins/pin_1000")
    df.show(10, True)
