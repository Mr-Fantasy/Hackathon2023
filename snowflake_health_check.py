import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from awsglue.job import Job
import time

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import boto3
import glob,os

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Your PySpark code here
# For example, read data from S3 and write it back to another location
df_bhav_meta = spark.read.csv("s3://s3-bucket-hackathon/nse_metadata/NSE_Metadata_YahooFinance.csv",header=True, inferSchema=True)

df_nse_meta = spark.read.csv("s3://s3-bucket-hackathon/nse_metadata/NSE_mktData_Metadata.csv",header=True, inferSchema=True)

nse_df1 = spark.read.csv("s3://s3-bucket-hackathon/NSE_market_data/data/August282023/10.mkt.csv",header=True, inferSchema=True)
df1_filter = nse_df1[nse_df1['Total_Traded_Quantity']>=5000]


bhav_df = spark.read.csv("s3://s3-bucket-hackathon/NSE_BHAV_data/yfinance_historical_stockprice.csv",header=True, inferSchema=True)
len_bhav_s3 = bhav_df.count()

# Snowflake
sfOptions = {
"sfURL" : "tlktcnk-kq18739.snowflakecomputing.com/",
"sfUser" : "sushma",
"sfPassword" : "Maveric@123",
"sfDatabase" : "test_db",
"sfSchema" : "public",
"sfWarehouse" : "compute_wh",
"application" : "AWSGlue"
}

snowflake_source = "net.snowflake.spark.snowflake"

nse_df_snow = spark.read \
    .format(snowflake_source) \
    .options(**sfOptions) \
    .option("dbtable", "PASSED_QUALITY_CHECK_RECORDS_SOURCE_STAGE") \
    .load()

def health_check(source,target):
    if source == target:
        return "The number of rows in S3 is same in Snowflake"
    else:
        return f"Missing number of rows is -> {source-target}"
 
snow_bhav_count = nse_df_snow.count()

health_status = health_check(len_bhav_s3,snow_bhav_count)
print(health_status)

def transform_data(df):
    df = df.withColumn("spread_high_low", df['low']-df['high'])
    df = df.withColumn("spread_open_close", df['close']-df['open'])
    df = df.withColumn("returns_perc", ((df['close'] - df['prevclose']) * 100) / df['prevclose'])
    
    monthly_window = Window.partitionBy("symbol", year("timestamp"), month("timestamp")).orderBy("timestamp")
    
    df = df.withColumn("open_monthly", first("open").over(monthly_window))
    df = df.withColumn("close_monthly", last("close").over(monthly_window))
    df = df.withColumn("high_monthly", max("high").over(monthly_window))
    df = df.withColumn("low_monthly", min("low").over(monthly_window))
    
    return df

def datatype_check(metadata,target_df):
    pass

job.commit()