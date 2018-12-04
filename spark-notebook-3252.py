# Databricks notebook source
# Keys removed

from pyspark.sql.types import *

pythonSchema = StructType().add("created_at", TimestampType()) \
          .add("id", StringType()) \
          .add ("text", StringType())

# COMMAND ----------

awsAccessKey = "KEY"
awsSecretKey = "KEY"

kinesisDF = spark \
  .readStream \
  .format("kinesis") \
  .option("streamName", "ingest-3252") \
  .option("initialPosition", "earliest") \
  .option("region", "us-east-1") \
  .option("awsAccessKey", awsAccessKey) \
  .option("awsSecretKey", awsSecretKey) \
  .load()

#display(kinesisDF)

# COMMAND ----------

from pyspark.sql.functions import *

dataDF = kinesisDF \
  .selectExpr("cast (data as STRING) jsonData") \
  .select("jsonData")

display(dataDF)

# COMMAND ----------

dataDF.writeStream \
    .outputMode("append") \
    .queryName("writing_to_es") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/") \
    .option("es.resource", "index/type") \
    .option("es.nodes", "search-project-3252-aurq5tobzyh6s6rh5axcccgw3i.us-east-1.es.amazonaws.com") \
    .option("es.port", "80") \
    .option("es.nodes.wan.only", "true") \
    .start()
