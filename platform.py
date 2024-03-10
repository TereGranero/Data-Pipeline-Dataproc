"""
Generates two monthly reports of resources duration usage of platform:  
one by country and one by time zone. 
Stores them in Cloud Storage 
in Parquet
"""
#-------------------------- IMPORT LIBRARIES ----------------------------------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


#------------------------- START SPARK SESSION --------------------------------

spark = SparkSession.builder.master("local")\
    .appName("top10")\
    .getOrCreate()


#----------------------------- LOAD DATA --------------------------------------

# Define event schema
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

# Load JSONL data into Spark DataFrame
events_df = spark.read.json("data/eventos.jsonl", schema=events_schema)

# Register table alias to allow SQL use
events_df.createOrReplaceTempView("events")

# Select the columns we need
events_df = spark.sql("SELECT eventId, eventTime, resourceId, countryCode, duration from events")


#-------------------------- EXTRACT SOME DATA ---------------------------------

# Extract month from 'eventTime' column
events_df = events_df\
    .withColumn("month", F.substring(F.col("eventTime"), 1, 7))
    
# Extract timezone from 'eventTime' column
events_df = events_df\
    .withColumn("timeZone", F.substring(F.col("eventTime"), 20, 6))



#--------------------------------- UDFS ---------------------------------------

# UDF to calculate resource total usage vs. all resources total usage in % 
spark.udf.register('usage_percent_total_udf', 
                   lambda total_duration_resource, total_duration_all: 
                        (total_duration_resource / total_duration_all) * 100, 
                   DoubleType())


#------------------------------ CALCULATIONS ----------------------------------

# Sum duration by month and resourceId -> totalDurationResource
grouped_df = events_df\
    .groupBy("month", "resourceId")\
    .agg(F.sum("duration").alias("totalDurationResource"))

# Join the DataFrame with the totalDurationResource column
monthly_resources_usage_df = events_df\
    .join(grouped_df, on=["month", "resourceId"], how="left")\



# Sum all totalDurationResource by month -> totalDurationAll
total_duration_all_df = monthly_resources_usage_df\
    .groupBy("month")\
    .agg(F.sum("duration").alias("totalDurationAll"))

# Join the DataFrame with the totalDurationAll column
monthly_resources_usage_df = monthly_resources_usage_df.\
    join(total_duration_all_df, on="month", how="left")
    

# Sum duration by month, country and resourceId -> totalDurationResourceByCountry
grouped_country_df = monthly_resources_usage_df\
    .groupBy("month", "countryCode", "resourceId")\
    .agg(F.sum("duration").alias("totalDurationResourceByCountry"))

# Join the DataFrame with the totalDurationResourceByCountry column
monthly_resources_usage_df = monthly_resources_usage_df\
    .join(grouped_country_df, on=["month", "countryCode", "resourceId"], how="left")
    

# Sum all totalDurationResource by month and country -> totalDurationResourceAllByCountry
grouped_country_all_df = monthly_resources_usage_df\
    .groupBy("month", "countryCode")\
    .agg(F.sum("duration").alias("totalDurationResourceAllByCountry"))

# Join the DataFrame with the totalDurationResourceAllByCountry column
monthly_resources_usage_df = monthly_resources_usage_df\
    .join(grouped_country_all_df, ["month", "countryCode"], how="left")


# Sum duration by month, timeZone and resourceId -> totalDurationResourceByZone
grouped_zone_df = monthly_resources_usage_df\
    .groupBy("month", "timeZone", "resourceId")\
    .agg(F.sum("duration").alias("totalDurationResourceByzone"))

# Join the DataFrame with the totalDurationResourceByZone column
monthly_resources_usage_df = monthly_resources_usage_df\
    .join(grouped_zone_df, on=["month", "timeZone", "resourceId"], how="left")


# Sum all totalDurationResource by month and time zone -> totalDurationResourceAllByZone
grouped_zone_all_df = monthly_resources_usage_df\
    .groupBy("month", "timeZone")\
    .agg(F.sum("duration").alias("totalDurationResourceAllByZone"))

# Join the DataFrame with the totalDurationResourceAllByCountry column
monthly_resources_usage_df = monthly_resources_usage_df.join(grouped_zone_all_df, ["month", "timeZone"], how="left")



#--------------------------- ITERATION BY MONTH -------------------------------

# Register table alias to allow SQL use
monthly_resources_usage_df.createOrReplaceTempView("usage")

# Collect distinct months into a list
month_list = [row.month for row in monthly_resources_usage_df.select("month").distinct().collect()]

for one_month in month_list:

    # Prepare monthly report by country
    output_country_df = spark\
        .sql(f"SELECT month,\
                resourceId,\
                countryCode,\
                usage_percent_total_udf(totalDurationResource,totalDurationAll) AS usagePercentTotal,\
                usage_percent_total_udf(totalDurationResourceByCountry, totalDurationResourceAllByCountry) AS usagePercentRelativeCountry,\
                totalDurationResource AS totalDurationInSec \
                FROM usage \
                WHERE usage.month = '{one_month}'")
    
    # Prepare monthly report by time zone
    output_zone_df = spark\
        .sql(f"SELECT month,\
                    resourceId,\
                    timeZone,\
                    usage_percent_total_udf(totalDurationResource, totalDurationAll) AS usagePercentTotal,\
                    usage_percent_total_udf(totalDurationResourceByZone, totalDurationResourceAllByZone) AS usagePercentRelativeTz,\
                    totalDurationResource AS totalDurationInSec \
                FROM usage \
                WHERE usage.month = '{one_month}'")
    
    
    # Drop duplicates
    output_country_df = output_country_df\
        .dropDuplicates(['month', 'resourceId', 'countryCode'])
    
    output_zone_df = output_zone_df\
        .dropDuplicates(['month', 'resourceId', 'timeZone'])
        

    # Two monthly reports are generated
    report_file_name_country = f'usage_country_{one_month}_report.parquet'
    output_country_df.write.mode("overwrite").parquet(report_file_name_country)

    report_file_name_zone = f'usage_zone_{one_month}_report.parquet'
    output_zone_df.write.mode("overwrite").parquet(report_file_name_zone)
    
    """
    report_file_name_country = f'usage_country_{one_month}_report.csv'
    output_country_df.write.csv(report_file_name_country, sep="|", header=True, mode="overwrite")

    report_file_name_zone = f'usage_zone_{one_month}_report.csv'
    output_zone_df.write.csv(report_file_name_zone, sep="|", header=True, mode="overwrite")
    """

# ------------------------------ STOP SPARK SESSION ---------------------------

spark.stop()
