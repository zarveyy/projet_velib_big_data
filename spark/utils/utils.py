from pyspark.sql.functions import col, from_json, avg, window

def average(df): # Every 10 minutes, produce a day windowed df with average elec and mechanical bikes and anverage number of docks available by station 
  return df \
  .groupby(window(col('timestamp'), "60 seconds", "60 seconds"), col('stationcode')) \
  .agg(avg('mechanical').alias('avg_mechanical'), avg('ebike').alias('avg_ebike'),  avg('numdocksavailable').alias('avg_numdocksavailable'))

def kafkaDataframe(df, schema): # Return a parsed json string kafka df
  return df.select(from_json(col('value'), schema).alias('jsonData')).select('jsonData.*')