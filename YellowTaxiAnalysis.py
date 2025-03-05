# Databricks notebook source
#Importing Datasets
data2015_1 = spark.read.csv('dbfs:/FileStore/yellow_tripdata_2015_01.csv', header=True, inferSchema=True)

# COMMAND ----------

#Displaying Schema
print("Schema for 2015_1")
data2015_1.printSchema()

# COMMAND ----------

columns = [
    'VendorID', 'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 
    'RateCodeID', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 
    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
    'improvement_surcharge', 'total_amount'
]

dataset = data2015_1
print("Basic statistics for data2015_1:")
pddf = dataset.select(columns).describe().toPandas()
pddf.set_index('summary')
pddf



# COMMAND ----------

from pyspark.sql.functions import col,unix_timestamp
cleancolumns = ["fare_amount","trip_distance","passenger_count"]

#Cleaning Columns
data2015_1c = data2015_1.na.drop(subset=cleancolumns)
data2015_1c = data2015_1c.filter((data2015_1c['fare_amount'] >= 0) & (data2015_1c['trip_distance'] > 0))
data2015_1c = data2015_1c.withColumn('tpep_pickup_datetime', col('tpep_pickup_datetime').cast('timestamp')) \
.withColumn('tpep_dropoff_datetime', col('tpep_dropoff_datetime').cast('timestamp'))
#Refactoring
data2015_1c = data2015_1c.withColumn('trip_duration', (unix_timestamp('tpep_dropoff_datetime') - unix_timestamp('tpep_pickup_datetime')) / 60)
data2015_1c = data2015_1c.withColumn('trip_speed', col('trip_distance') / (col('trip_duration') / 60))



# COMMAND ----------

from pyspark.sql.functions import avg,hour,count
from pyspark.sql import DataFrame

# Load the dataset
dta = data2015_1c

# Displays Average fare and trip distance
print("Average fare and trip distance for data2015_1:")
dta.groupBy('passenger_count').agg(
    avg('fare_amount').alias('avg_fare_amount'),
    avg('trip_distance').alias('avg_trip_distance')
).show()

# Displays Busiest times for pickup
print("Busiest times of day for pickups in data2015_1:")
dta.withColumn('pickup_hour', hour('tpep_pickup_datetime')) \
    .groupBy('pickup_hour') \
    .agg(count('*').alias('pickup_count')) \
    .orderBy('pickup_count', ascending=False) \
    .show()

# Displays Longitude + Latitude numbers for the highest average amount
print("Neighbourhoods with highest average fare amount in data2015_1:")
dta.groupBy('pickup_latitude', 'pickup_longitude') \
    .agg(avg('fare_amount').alias('avg_fare_amount')) \
    .orderBy('avg_fare_amount', ascending=False) \
    .show()



# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Displaying Distribution of Trip Distances
trip_distance_pd = dta.select('trip_distance').toPandas()
filtered_trip_distance = trip_distance_pd[trip_distance_pd['trip_distance'] <= 10]

plt.figure(figsize=(10, 6))
plt.hist(filtered_trip_distance['trip_distance'], bins=20, alpha=0.7, color='blue')
plt.title('Distribution of Trip Distances for data2015_1')
plt.xlabel('Trip Distance (miles)')
plt.ylabel('Frequency')
plt.grid(axis='y')
plt.xlim(0, 10)
plt.show()

# Displaying Average Fares per Hour
avg_fare_by_hour = dta.withColumn('pickup_hour', hour('tpep_pickup_datetime')) \
    .groupBy('pickup_hour') \
    .agg(avg('fare_amount').alias('avg_fare_amount')) \
    .orderBy('pickup_hour') \
    .toPandas()

plt.figure(figsize=(10, 6))
sns.barplot(x='pickup_hour', y='avg_fare_amount', data=avg_fare_by_hour, palette='coolwarm')
plt.title('Average Fare by Hour of the Day for data2015_1')
plt.xlabel('Hour of the Day')
plt.ylabel('Average Fare Amount')
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.show()





# COMMAND ----------

# MAGIC %md
# MAGIC Summary of Basic Prescriptive Analytics
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Part B
# PartB Q1 done while calculating average speed
from pyspark.sql.functions import hour, dayofweek

data2015_1c = data2015_1c.withColumn('pickup_hour', hour('tpep_pickup_datetime')) \
    .withColumn('pickup_day', dayofweek('tpep_pickup_datetime'))



# COMMAND ----------

from pyspark.sql.functions import dayofmonth

data2015_1c = data2015_1c.withColumn('day', dayofmonth('tpep_pickup_datetime'))
trend = data2015_1c.groupBy('day') \
    .agg(avg('trip_duration').alias('avg_trip_duration')) \
    .orderBy('day').toPandas()

plt.figure(figsize=(10, 6))
sns.barplot(x='day', y='avg_trip_duration', data=trend, palette = 'Blues_d')
plt.title('Average Trip Duration by Day (January 2015)')
plt.xlabel('Day of the Month')
plt.ylabel('Average Trip Duration (minutes)')
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.show()






# COMMAND ----------

hourly_trip_duration = data2015_1c.groupBy('pickup_hour') \
    .agg(avg('trip_duration').alias('avg_trip_duration')) \
    .orderBy('pickup_hour').toPandas()

plt.figure(figsize=(10, 6))
sns.barplot(x='pickup_hour', y='avg_trip_duration', data=hourly_trip_duration, palette='coolwarm')
plt.title('Average Trip Duration by Hour of the Day')
plt.xlabel('Hour of the Day')
plt.ylabel('Average Trip Duration (minutes)')
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.show()

