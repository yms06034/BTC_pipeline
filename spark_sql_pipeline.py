from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

conf = SparkConf()

conf.set("spark.hadoop.fs.s3a.access.key", "AKIAZ5URTMMXVPE4ZB55")
conf.set("spark.hadoop.fs.s3a.secret.key", "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")

conf.set("spatk.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")

conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
         'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

spark = SparkSession.builder.config(conf=conf).appName("log_to_spark").getOrCreate()


spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

log_df = spark.read.format("csv") \
            .option("header", "true") \
            .option('infreSchema', 'true') \
            .load("s3a://depipe/logfile.csv")