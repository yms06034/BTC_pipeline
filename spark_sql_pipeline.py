from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime
import os

suffix = datetime.now().strftime("%y%m%d")

MAX_MENORY = '5g'

conf = SparkConf()

conf.set("spark.hadoop.fs.s3a.access.key", "CKIAD5URTWQXVPE4ZB55")
conf.set("spark.hadoop.fs.s3a.secret.key", "TdwjfDJsd2ft3ax/T2difLawdhuiTQvb05SKjdwijskdal")
conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
         'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

conf.set("spark.executor.memory", MAX_MENORY)
conf.set("spark.driver.memory", MAX_MENORY)
conf.set("spark.jars", "/home/yms06034/spark/spark3/mysql-connector-java-8.0.29.jar")


spark = SparkSession.builder.config(conf=conf).appName("log_to_spark").getOrCreate()


spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

log_df = spark.read.format("csv") \
            .option("header", "true") \
            .option('infreSchema', 'true') \
            .load("s3a://log-to-spark/{0}.csv")

log_df.createOrReplaceTempView("log")

query = """
SELECT 
  *
FROM 
  log
"""

log_dd = spark.sql(query)
save_dir = "/home/sungjin/airflow/data/"

log_dd.write.format("parquet").mode("overwrite").save(f"{save_dir}/log_{suffix}/")

log_dd.select("*").write.format("jdbc").mode(saveMode="org.apache.spark.sql.SaveMode.Append") \
      .option("driver", "com.mysql.jdbc.Driver") \
      .option("user", "root").option("password", "") \
      .jdbc(f"jdbc:mysql://localhost:3306/spark_db", "log_{suffix}")
