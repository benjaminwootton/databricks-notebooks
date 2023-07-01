# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming With PySpark
# MAGIC This notebook demonstrates how to use structured streaming from a Kafka source.
# MAGIC It demonstrates simple filters and aggregations, then moves into more complex stateful transformations with windowing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream events from Kafka
# MAGIC Read stream from Kafka orders topic.
# MAGIC This notebook assumes events in the following JSON structure:
# MAGIC { order_id : 1, category : 'Homeware', value : 23.34, timestamp : 343443434 }

# COMMAND ----------

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "54.217.155.51:9092") \
    .option("subscribe", "orders") \
    .load() \
    .selectExpr( "CAST(key AS STRING)", "CAST(value AS STRING)" )

# COMMAND ----------

# MAGIC %md
# MAGIC Explode JSON into the dataframe

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([
    StructField("order_id", StringType()),
    StructField("category", StringType()),
    StructField("value", DoubleType()),
    StructField("timestamp", StringType())
])

dforders = df.withColumn( "value_json", lit( from_json( col( "value" ), schema ))) \
        .select( "value_json.*")

display( dforders )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Dataframe

# COMMAND ----------

display( dforders )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Count

# COMMAND ----------

display( dforders.agg( count("*")) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Aggregations

# COMMAND ----------

display( \
    dforders.select( "category", "value" ) \
        .groupBy( "category" ) \
        .agg( sum( "value" ) )
)

display( \
    dforders.select( "category", "value" ) \
        .groupBy( "category" ) \
        .agg( avg( "value" ) )
)
