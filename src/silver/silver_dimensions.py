# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys

project_path = os.path.join(os.getcwd(),"..","..")
sys.path.append(project_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dim User

# COMMAND ----------

# MAGIC %md
# MAGIC ### AutoLoader
# MAGIC

# COMMAND ----------

df_user = spark.readStream.format('cloudFiles')\
               .option('cloudFiles.format','parquet')\
               .option('cloudFiles.schemaLocation',"abfss://silver@saispotifystorage.dfs.core.windows.net/DimUser/checkpoint")\
               .load("abfss://bronze@saispotifystorage.dfs.core.windows.net/DimUser")

# COMMAND ----------

df_user = df_user.withColumn("user_name",upper(col("user_name")))
display(
    df_user,
    checkpointLocation="abfss://silver@saispotifystorage.dfs.core.windows.net/DimUser/checkpoint_v2"
)

# COMMAND ----------

from utils.transformations import reusable

df_user_obj = reusable()

df_user_del = df_user_obj.dropColumns(df_user,['_rescued_data'])  # Ensure 'dropColumns' method exists in 'resuable' class

df_user_del = df_user_del.dropDuplicates(['user_id'])
display(
    df_user_del,
    checkpointLocation="abfss://silver@saispotifystorage.dfs.core.windows.net/DimUser/checkpoint_v3"
)

# COMMAND ----------

df_user_del.writeStream.format("delta")\
                       .outputMode("append")\
                       .option("checkpointLocation","abfss://silver@saispotifystorage.dfs.core.windows.net/DimUser/checkpoint_v4")\
                       .trigger(once=True)\
                       .option("path","abfss://silver@saispotifystorage.dfs.core.windows.net/DimUser/data")\
                       .toTable("spotify_cata.silver.DimUser")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dim Artist

# COMMAND ----------

df_artist = spark.readStream.format('cloudFiles')\
               .option('cloudFiles.format','parquet')\
               .option('cloudFiles.schemaLocation',"abfss://silver@saispotifystorage.dfs.core.windows.net/DimArtist/checkpoint")\
               .load("abfss://bronze@saispotifystorage.dfs.core.windows.net/DimArtist")

# COMMAND ----------

display(df_artist,checkpointLocation="abfss://silver@saispotifystorage.dfs.core.windows.net/DimArtist/checkpoint/v0")

# COMMAND ----------

from utils.transformations import reusable

df_artist_obj = reusable()

df_artist_del = df_artist_obj.dropColumns(df_artist,['_rescued_data'])  # Ensure 'dropColumns' method exists in 'resuable' class

df_artist_del = df_artist_del.dropDuplicates(['artist_id'])

df_artist_del.writeStream.format("delta")\
                       .outputMode("append")\
                       .option("checkpointLocation","abfss://silver@saispotifystorage.dfs.core.windows.net/DimArtist/checkpoint/v1")\
                       .trigger(once=True)\
                       .option("path","abfss://silver@saispotifystorage.dfs.core.windows.net/DimArtist/data")\
                       .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dim Track

# COMMAND ----------

df_track = spark.readStream.format('cloudFiles')\
               .option('cloudFiles.format','parquet')\
               .option('cloudFiles.schemaLocation',"abfss://silver@saispotifystorage.dfs.core.windows.net/DimTrack/checkpoint")\
               .load("abfss://bronze@saispotifystorage.dfs.core.windows.net/DimTrack")

display(df_track,checkpointLocation="abfss://silver@saispotifystorage.dfs.core.windows.net/DimTrack/checkpoint/v0")

# COMMAND ----------

from utils.transformations import reusable

df_track_obj = reusable()

df_track_del = df_track_del.withColumn("durationFlag",when(col("duration_sec")<150,"low")
                                                     .when(col("duration_sec")<300,"medium")
                                                     .otherwise("high"))


df_track_del = df_track_del.withColumn("track_name",regexp_replace(col("track_name"),"-"," "))

df_track_del = df_track_obj.dropColumns(df_track,['_rescued_data'])  # Ensure 'dropColumns' method exists in 'resuable' class

df_track_del = df_track_del.dropDuplicates(['track_id'])


df_track_del.writeStream.format("delta")\
                       .outputMode("append")\
                       .option("checkpointLocation","abfss://silver@saispotifystorage.dfs.core.windows.net/DimTrack/checkpoint/v1")\
                       .trigger(once=True)\
                       .option("path","abfss://silver@saispotifystorage.dfs.core.windows.net/DimTrack/data")\
                       .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dim Date

# COMMAND ----------

df_date = spark.readStream.format('cloudFiles')\
               .option('cloudFiles.format','parquet')\
               .option('cloudFiles.schemaLocation',"abfss://silver@saispotifystorage.dfs.core.windows.net/DimDate/checkpoint")\
               .load("abfss://bronze@saispotifystorage.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date = reusable().dropColumns(df_date,['_rescued_data'])
df_date.writeStream.format("delta")\
                       .outputMode("append")\
                       .option("checkpointLocation","abfss://silver@saispotifystorage.dfs.core.windows.net/DimDate/checkpoint/v1")\
                       .trigger(once=True)\
                       .option("path","abfss://silver@saispotifystorage.dfs.core.windows.net/DimDate/data")\
                       .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ##FactStream

# COMMAND ----------

df_fact = spark.readStream.format('cloudFiles')\
               .option('cloudFiles.format','parquet')\
               .option('cloudFiles.schemaLocation',"abfss://silver@saispotifystorage.dfs.core.windows.net/FactStream/checkpoint")\
               .load("abfss://bronze@saispotifystorage.dfs.core.windows.net/FactStream")



# COMMAND ----------

df_fact = reusable().dropColumns(df_fact,['_rescued_data'])
df_fact.writeStream.format("delta")\
                       .outputMode("append")\
                       .option("checkpointLocation","abfss://silver@saispotifystorage.dfs.core.windows.net/FactStream/checkpoint/v1")\
                       .trigger(once=True)\
                       .option("path","abfss://silver@saispotifystorage.dfs.core.windows.net/FactStream/data")\
                       .toTable("spotify_cata.silver.FactStream")

# COMMAND ----------



# COMMAND ----------

