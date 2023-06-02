from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, ArrayType
from utils.utils import average, kafkaDataframe
from time import sleep

# Créer un SparkSession
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR") # Less logs

station_cluster_scheme = StructType([ \
  StructField('id_station', StringType(), False), \
  StructField('cluster_label', IntegerType(), False)
])

velib_fields_scheme = StructType([ \
  StructField('name', StringType(), False), \
  StructField('stationcode', StringType(), False), \
  StructField('ebike', IntegerType(), False), \
  StructField('mechanical', IntegerType(), False), \
  StructField('coordonnees_geo', ArrayType(FloatType()), False), \
  StructField('numbikesavailable', IntegerType(), False), \
  StructField('numdocksavailable', IntegerType(), False), \
  StructField('capacity', IntegerType(), False), \
  StructField('is_renting', StringType(), False), \
  StructField('is_installed', StringType(), False), \
  StructField('nom_arrondissement_communes', StringType(), False), \
  StructField('is_returning', StringType(), False), \
  StructField('timestamp', TimestampType(), False)  
])

# Lire les données de Kafka
streaming_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "velib_data") \
    .load()

string_casted_df = streaming_data.selectExpr("CAST(value AS STRING)") # Casting binary values to string
df = kafkaDataframe(string_casted_df, velib_fields_scheme) # Parse string values to a structured df 

new_df = average(df)

new_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .start() \

spark.streams.awaitAnyTermination() # Wait for the termination of the stream
