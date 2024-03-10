from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

"""
Generar mediante Spark un informe diario 
de los 10 artículos más ventas 
por día y por categoría. 

Guardar en un bucket de Cloud Storage en formato CSV
con la siguiente estructura:
position|date|categoryId|categoryName|resourceId|resourceName

La definición de las categorías puede extraerse de 
http://singularity.luisbelloch.es/v1/stripe/categories al inicio del proceso.
"""

# Start Spark session
spark = SparkSession.builder.master("local")\
    .appName("top10")\
    .getOrCreate()

# Define data schemas
events_schema = StructType([
    StructField("eventId", StringType(), True),
    StructField("eventTime", StringType(), True),
    StructField("processTime", StringType(), True),
    StructField("resourceId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("countryCode", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("itemPrice", StringType(), True)
])

resources_schema = StructType([
    StructField("id", StringType(), True),
    StructField("tenant", StringType(), True),
    StructField("name", StringType(), True),
    StructField("categoryId", StringType(), True),
    StructField("providerId", StringType(), True),
    StructField("promotion", StringType(), True),
    StructField("externalId", StringType(), True)
])

categories_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True)
])

# Load JSONL data into Spark DataFrame
events_df = spark.read.json("data/eventos.jsonl", schema=events_schema)
resources_df = spark.read.json("data/resources.jsonl", schema=resources_schema)
categories_df = spark.read.json("data/categories.jsonl", schema=categories_schema)

# Rename columns
resources_df = resources_df.withColumnRenamed("name", "resourceName")
resources_df = resources_df.withColumnRenamed("id", "resourceId")
categories_df = categories_df.withColumnRenamed("name", "categoryName")
categories_df = categories_df.withColumnRenamed("id", "categoryId")

#Normalize categoryId adding "0" after the dot
categories_df = categories_df.withColumn("categoryId", F.regexp_replace(F.col("categoryId"), r'\.(?=\d)', '.0'))

# Register table alias to allow SQL use
events_df.createOrReplaceTempView("events")
resources_df.createOrReplaceTempView("resources")
categories_df.createOrReplaceTempView("categories")

# Select the columns we need
events_df = spark.sql("SELECT eventId, processTime, resourceId from events")
resources_df = spark.sql("SELECT resourceId, resourceName, categoryId from resources")
categories_df = spark.sql("SELECT categoryId, categoryName from categories")

# Extract first 10 characters from the 'processTime' column to "yyyy-mm-dd"
events_df = events_df\
    .withColumn("processTime", F.substring(F.col("processTime"), 1, 10))
events_df = events_df\
    .withColumnRenamed("processTime", "date")

# Join events with resources to get resource names and category id
joined_df = events_df.\
    join(resources_df, on="resourceId", how="left")

# Join with categories to get category name
joined_df = joined_df.\
    join(categories_df, on="categoryId", how="left")
    
# Group by date and resourceId, then count occurrences
purchase_count_df = events_df.groupBy("date", "resourceId").count()
purchase_complete_df = joined_df.join(purchase_count_df, on=["date", "resourceId"], how="left")

# Rank 10 resources based on purchase count by category and date
windowSpec = Window.partitionBy("date", "categoryId").orderBy(F.col("count").desc())
ranked_df = purchase_complete_df\
    .withColumn("position", F.dense_rank().over(windowSpec))\
    .filter(F.col("position") <= 10)\
    .dropDuplicates(['date', 'categoryId', 'resourceId'])\
    .orderBy('date', 'categoryId', 'position')

# Collect distinct dates into a list
date_list = [row.date for row in events_df.select("date").distinct().collect()]

for one_day in date_list:
    # Filter by date
    ranked_df.createOrReplaceTempView("ranked")
    output_df = spark.sql(f"SELECT position, categoryId, categoryName, resourceId, resourceName FROM ranked WHERE ranked.date = '{one_day}'")

    # Selecting fields
    #ranked_df = ranked_df.select("position", "categoryId", "categoryName", "resourceId", "resourceName")

    # One report by day is generated in CSV
    report_file_name = f'top10_{one_day}_report.csv'
    output_df.write.csv(report_file_name, sep="|", header=True, mode="overwrite")

# Stop SparkSession
spark.stop()