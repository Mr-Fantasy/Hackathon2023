from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
# java_import(spark._jvm, SNOWFLAKE_SOURCE_NAME)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



@udf(returnType=BooleanType())
def validateDecimalLength(value, maxDigits, decimal_places):
    
    try:
        value_str = str(value)
        
        if '.' in value_str:
            digits, decimals = value_str.split('.')

            if len(digits)+len(decimals) <= int(maxDigits) and len(decimals) <= int(decimal_places):
                
                return True
        else:
            if len(value_str) <= int(maxDigits):
                
                return True
        
        return False

    except ValueError:
        return False



def healthCheck(sourceSchema,target_DF):
    pass

def nullDataValidation(records, fieldName, sourceTableName, targetTableName, sourcePrimaryKey):
    reportColumns = expr("map(" + ", ".join([f"'{col_name}', {col_name}" for col_name in records.columns]) + ")").alias("Unmatched Records")

    null_rows_DF = records.where(col(fieldName).isNull()).select(lit(f'Primary Key - Null Data Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"),lit(", ".join(sourcePrimaryKey)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))
    target_DF = records.where(col(fieldName).isNotNull())
    
    return null_rows_DF, target_DF



def duplicateDataValidation(records, fieldName, sourceTableName, targetTableName):
    reportColumns = expr("map(" + ", ".join([f"'{col_name}', {col_name}" for col_name in records.columns]) + ")").alias("Unmatched Records")
    
    duplicate_rows_DF = records.exceptAll(records.dropDuplicates(fieldName)).select(lit(f'Primary Key - Duplicate Data Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"),lit(", ".join(fieldName)).alias("Primary Key Field(s)"),lit(", ".join(fieldName)).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))
    # print(duplicate_rows_DF.show())
    

    return duplicate_rows_DF



def dataTypeValidation(spark,dataType, records, fieldName, sourceTableName, targetTableName, sourceMandatoryFields, primaryKeys, dateFormat=None):

    if fieldName in primaryKeys: iterable = primaryKeys
    else: iterable = primaryKeys + [fieldName]

    reportColumns = expr("map(" + ", ".join([f"'{col_name}', {col_name}" for col_name in iterable]) + ")").alias("Unmatched Records")

    if dataType.upper().strip() in ['DATE']:

        #   Date format validation

        if dateFormat == 'MM-YYYY-DD': actual_date_format = 'MM-yyyy-dd'
        elif dateFormat == 'YYYY-MM-DD': actual_date_format = 'yyyy-MM-dd'
        elif dateFormat == 'YY/MM/DD': actual_date_format = 'yy/MM/dd'
        elif dateFormat == 'DD/MM/YYYY': actual_date_format = 'dd/MM/yyyy'
        elif dateFormat == 'DD-MON-YY HH:MI:SS': actual_date_format = 'dd-MMM-yy HH:mm:ss'
        elif dateFormat == 'DD-MON-YYYY': actual_date_format = 'dd-MMM-yyyy'
        else:
            invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'Date Format Validation - New Format').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))
            return invalid_records_df
        print(actual_date_format)

        try:
            if fieldName in sourceMandatoryFields:
                incorrect_date_rows_DF = records.filter(to_date(col(fieldName), actual_date_format).isNull()).select(lit(f'Date Format Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))
            else:
                incorrect_date_rows_DF = records.filter(to_date(col(fieldName), actual_date_format).isNull() & col(fieldName).isNotNull()).select(lit(f'Date Format Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))


            return incorrect_date_rows_DF.limit(100)

        except Exception as e:

            # logger.error(f"An error occurred: {traceback.format_exc()}")

            invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataType - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))

            return invalid_records_df


    elif "VARCHAR" in dataType.upper().strip() or "CHAR" in dataType.upper().strip() or "STRING" in dataType.upper().strip():

        #   String Validation (with only certain allowed special charecters)

        try:
            allowed_regex_pattern = r"^[a-zA-Z0-9 ]*$"

            if fieldName in sourceMandatoryFields:
                incorrect_string_rows_DF = records.filter(~col(fieldName).rlike(allowed_regex_pattern) | (col(fieldName).isNull())).select(lit(f'DataType - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))
            else:
                incorrect_string_rows_DF = records.filter(~col(fieldName).rlike(allowed_regex_pattern)).select(lit(f'DataType - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))
            
            return incorrect_string_rows_DF

        except Exception as e:

            # logger.error(f"An error occurred: {traceback.format_exc()}")

            invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataType - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))

            return invalid_records_df


    elif "INTEGER" in dataType.upper().strip() or "INT" in dataType.upper().strip():

        #   Integer number Validation (not allowing decimal values)

        try:
            allowed_regex_pattern = r"^[+-]?\d+$"

            if fieldName in sourceMandatoryFields:
                incorrect_integer_rows_DF = records.filter(~col(fieldName).rlike(allowed_regex_pattern) | (col(fieldName).isNull())).select(lit(f'DataType - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))
            else:
                incorrect_integer_rows_DF = records.filter(~col(fieldName).rlike(allowed_regex_pattern)).select(lit(f'DataType - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))

            return incorrect_integer_rows_DF

        except Exception as e:
            invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataType - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))
            return invalid_records_df


    elif "NUMBER" in dataType.upper().strip() or "FLOAT" in dataType.upper().strip() or "DOUBLE" in dataType.upper().strip() or "DECIMAL" in dataType.upper().strip():

        #   Decimal number Validation (not allowing integer values)

        try:
            # allowed_regex_pattern = r"^\s*[-+]?\d*\.\d+\s*$"  # with leading and trailing spaces allowed
            allowed_regex_pattern = r"^[-+]?\d*\.\d+$" # with leading and trailing spaces not allowed

            if fieldName in sourceMandatoryFields:
                incorrect_decimal_rows_DF = records.filter(~col(fieldName).rlike(allowed_regex_pattern) | (col(fieldName).isNull())).select(lit(f'DataType - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))
            else:
                incorrect_decimal_rows_DF = records.filter(~col(fieldName).rlike(allowed_regex_pattern)).select(lit(f'DataType - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))

            return incorrect_decimal_rows_DF

        except Exception as e:
            invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataType - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))
            return invalid_records_df


    else:
        invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))

        return invalid_records_df



def dataLengthValidation(spark,records, fieldName, dataType, sourceTableName, targetTableName, primaryKeys):
    
    if fieldName in primaryKeys: iterable = primaryKeys
    else: iterable = primaryKeys + [fieldName]

    reportColumns = expr("map(" + ", ".join([f"'{col_name}', {col_name}"  for col_name in iterable]) + ")").alias("Unmatched Records")

    if "VARCHAR" in dataType.upper().strip() or "CHAR" in dataType.upper().strip() or "STRING" in dataType.upper().strip():

        try:
            dataLength = dataType.split('(')[-1][:-1]

            incorrect_string_length_rows_DF = records.where(length(col(fieldName)) > dataLength).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))

            
            return incorrect_string_length_rows_DF

        except Exception as e:


            invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))

            return invalid_records_df


    elif dataType.upper().strip() == "BIGINT":
        
        try:
            maxValue = len(str(np.iinfo(np.int64).max))

            incorrect_bigint_length_rows_DF = records.where((length(col(fieldName).cast("string")) > maxValue)).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))
            
            return incorrect_bigint_length_rows_DF

        except Exception as e:
            invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))
            return invalid_records_df


    elif dataType.upper().strip() == "INTEGER" or dataType.upper().strip() == "INT":

        try:
            maxValue = len(str(np.iinfo(np.int32).max))

            incorrect_integer_length_rows_DF = records.where((length(col(fieldName).cast("string")) > maxValue)).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))

            return incorrect_integer_length_rows_DF

        except Exception as e:

            invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))
            return invalid_records_df


    elif dataType.upper().strip() == "TINYINT":

        try:
            maxValue = len(str(np.iinfo(np.int8).max))

            incorrect_tinyint_length_rows_DF = records.where((length(col(fieldName).cast("string")) > maxValue.max)).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))
            
            return incorrect_tinyint_length_rows_DF

        except Exception as e:


            invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))

            return invalid_records_df


    elif "DECIMAL" in dataType.upper().strip() or "NUMBER" in dataType.upper().strip() or "FLOAT" in dataType.upper().strip() or "DOUBLE" in dataType.upper().strip():

        try:
            maxDigits, decimalLength = tuple(dataType.split('(')[-1][:-1].split(','))
        except ValueError:
            maxDigits, decimalLength = dataType.split('(')[-1][:-1].split(',')[0], '0'


        try:
            
            incorrect_decimal_length_rows_DF =  records.filter(~validateDecimalLength(col(fieldName),lit(maxDigits),lit(decimalLength))).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),reportColumns.astype("string").alias("Unmatched Records"))
            
            return incorrect_decimal_length_rows_DF

        except Exception as e:

            invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))

            return invalid_records_df


    elif dataType.upper().strip() == 'DATE':
        invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))

        return invalid_records_df


    else:

        invalid_records_df = spark.createDataFrame([Row()]).select(lit(f'DataLength - {dataType} Validation').alias("Validation Type"),lit(sourceTableName).alias("Source Table Name"),lit(targetTableName).alias("Target Table Name"), lit(", ".join(primaryKeys)).alias("Primary Key Field(s)"),lit(fieldName).alias("Validation Column"),lit("**ALL RECORDS**").alias("Unmatched Records"))

        return invalid_records_df


s3_client = boto3.client("s3")
bucket_name = 's3-bucket-hackathon'
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="nse_metadata")
files = response.get("Contents")
for file in files:
    if file["Key"] != "nse_metadata/" and file['Key'] == "nse_metadata/NSE_mktData_Metadata.csv":
        metadataTableRecords = pd.read_csv(f's3://{bucket_name}/{file["Key"]}')
        metadataTableRecords.columns = ['sl no', 'column_name', 'description', 'data_type', 'data_length', 'decimal_length', 'decimal_exists', 'start_position', 'end_position', 'date_format', 'primary_key', 'foreign_key', 'mandatory_flag', 'comments']
        sourcePrimaryKey = metadataTableRecords.loc[metadataTableRecords['primary_key'] == 'Y']['column_name'].tolist()
        sourceMandatoryFields = metadataTableRecords.loc[metadataTableRecords['mandatory_flag'] == 'Y']['column_name'].tolist()
    
        basePath = "s3a://{0}/NSE_market_data/data/".format(bucket_name)
        target_DF = spark.read.option("basePath","s3a://{0}/NSE_market_data/data/".format(bucket_name)).option("header","True").csv(f"{basePath}/**/*.csv")
        # print(target_DF.printSchema())
        print(f"Total Number of records in Staging is {target_DF.count()}")
        print(f"Total Number of columns in Staging is {len(target_DF.columns)}")
        nullColumns =  [c for c, const in target_DF.select([((min(c).isNull() & max(c).isNull()) | (min(c) == max(c))).alias(c) for c in target_DF.columns]).first().asDict().items() if const]
        print(f"Columns that have only NULL values in the data - {nullColumns}")
        sourceTableName = f"s3://{bucket_name}/NSE_market_data/data/*.csv"
        targetTableName = f"s3://{bucket_name}/NSE_market_data/data/*.csv"
        detailedMetadataValidationSchema = StructType([
                    StructField("Validation Type", StringType(), True),
                    StructField("Source Table Name", StringType(), True),
                    StructField("Target Table Name", StringType(), True),
                    StructField("Primary Key Field(s)", StringType(), True),
                    StructField("Validation Column" , StringType(), True),
                    StructField("Unmatched Records", StringType(), True),
                ])
        sourceSchema = metadataTableRecords
        detailedMetadataReport_DF = spark.createDataFrame([],schema=detailedMetadataValidationSchema)
        for key in sourcePrimaryKey:
            detailedReport_DF, target_DF = nullDataValidation(target_DF, key, sourceTableName, targetTableName, sourcePrimaryKey)
            detailedReport_DF.show()
            if detailedReport_DF.count() >0:
                detailedMetadataReport_DF = detailedMetadataReport_DF.unionByName(detailedReport_DF)
        print("Pk - Done")
        detailedReport_DF = duplicateDataValidation(target_DF, sourcePrimaryKey, sourceTableName, targetTableName)
        detailedMetadataReport_DF = detailedMetadataReport_DF.unionByName(detailedReport_DF)
        print("Duplicate Done")
        for column in target_DF.columns:
            dataType = sourceSchema.loc[sourceSchema['column_name'] == column, 'data_type'].item()
            detailedReport_DF = dataLengthValidation(spark,target_DF, column, dataType, sourceTableName, targetTableName, sourcePrimaryKey)
            if detailedReport_DF.count() > 0:
                detailedMetadataReport_DF = detailedMetadataReport_DF.unionByName(detailedReport_DF)
            if 'DATE' in dataType.upper().strip():
                dateFormat = eval(sourceSchema.loc[sourceSchema['column_name'] == column, 'date_format'].item())
                detailedReport_DF = dataTypeValidation(spark,dataType, target_DF, column, sourceTableName, targetTableName, sourceMandatoryFields, sourcePrimaryKey, dateFormat)
                if detailedReport_DF.count() > 0:
                    detailedMetadataReport_DF = detailedMetadataReport_DF.unionByName(detailedReport_DF)
            else:
                detailedReport_DF = dataTypeValidation(spark,dataType, target_DF, column, sourceTableName, targetTableName, sourceMandatoryFields, sourcePrimaryKey, dateFormat=None)
                if detailedReport_DF.count() > 0:
                    detailedMetadataReport_DF = detailedMetadataReport_DF.unionByName(detailedReport_DF)

        sfOptions = {
        "sfURL" : "tlktcnk-kq18739.snowflakecomputing.com/",
        "sfUser" : "sushma",
        "sfPassword" : "Maveric@123",
        "sfDatabase" : "test_db",
        "sfSchema" : "public",
        "sfWarehouse" : "compute_wh",
        "application" : "AWSGlue"
        }
        detailedMetadataReport_DF.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "Failed_Quality_Check_Records_Source_Stage_MktData").mode("overwrite").save()
    
        target_DF.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "Passed_Quality_Check_Records_Source_Stage_MktData").mode("overwrite").save()
    # elif file["Key"] != "nse_metadata/" and file['Key'] == "nse_metadata/NSE_Metadata_YahooFinance.csv":
    #     pass
    elif file["Key"] != "nse_metadata/" and file['Key'] == "nse_metadata/NSE_bhavdata_Metadata.csv":
        metadataTableRecords = pd.read_csv(f's3://{bucket_name}/{file["Key"]}')
        metadataTableRecords.columns = ['sl no', 'column_name', 'description', 'data_type', 'data_length', 'decimal_length', 'decimal_exists', 'start_position', 'end_position', 'date_format', 'primary_key', 'foreign_key', 'mandatory_flag', 'comments']
        sourcePrimaryKey = metadataTableRecords.loc[metadataTableRecords['primary_key'] == 'Y']['column_name'].tolist()
        sourceMandatoryFields = metadataTableRecords.loc[metadataTableRecords['mandatory_flag'] == 'Y']['column_name'].tolist()
        basePath = "s3a://{0}/NSE_BHAV_data/data/".format(bucket_name)
        target_DF = spark.read.option("basePath","s3a://{0}/NSE_BHAV_data/data/".format(bucket_name)).option("header","True").csv(f"{basePath}/*.csv")
        target_DF = target_DF.select("SYMBOL","SERIES","OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE","TOTTRDQTY","TOTTRDVAL",col("TIMESTAMP").alias("generated_date"),"TOTALTRADES","ISIN")
        # print(target_DF.printSchema())
        # historicalDF = historicalDF.unionByName(archivalDF)
        # target_DF= historicalDF
        print(f"Total Number of records in Staging is {target_DF.count()}")
        print(f"Total Number of columns in Staging is {len(target_DF.columns)}")
        nullColumns =  [c for c, const in target_DF.select([((min(c).isNull() & max(c).isNull()) | (min(c) == max(c))).alias(c) for c in target_DF.columns]).first().asDict().items() if const]
        print(f"Columns that have only NULL values in the data - {nullColumns}")
        sourceTableName = f"s3://{basePath}/*.csv"
        targetTableName = f"{basePath}/*.csv"
        detailedMetadataValidationSchema = StructType([
                    StructField("Validation Type", StringType(), True),
                    StructField("Source Table Name", StringType(), True),
                    StructField("Target Table Name", StringType(), True),
                    StructField("Primary Key Field(s)", StringType(), True),
                    StructField("Validation Column" , StringType(), True),
                    StructField("Unmatched Records", StringType(), True),
                ])
        sourceSchema = metadataTableRecords
        detailedMetadataReport_DF = spark.createDataFrame([],schema=detailedMetadataValidationSchema)
        for key in sourcePrimaryKey:
            detailedReport_DF, target_DF = nullDataValidation(target_DF, key, sourceTableName, targetTableName, sourcePrimaryKey)
            detailedReport_DF.show()
            if detailedReport_DF.count() >0:
                detailedMetadataReport_DF = detailedMetadataReport_DF.unionByName(detailedReport_DF)
        print("Pk - Done")
        detailedReport_DF = duplicateDataValidation(target_DF, sourcePrimaryKey, sourceTableName, targetTableName)
        detailedMetadataReport_DF = detailedMetadataReport_DF.unionByName(detailedReport_DF)
        print("Duplicate Done")
        for column in target_DF.columns:
            dataType = sourceSchema.loc[sourceSchema['column_name'] == column, 'data_type'].item()
            print(dataType)
            detailedReport_DF = dataLengthValidation(spark,target_DF, column, dataType, sourceTableName, targetTableName, sourcePrimaryKey)
            if detailedReport_DF.count() > 0:
                detailedMetadataReport_DF = detailedMetadataReport_DF.unionByName(detailedReport_DF)
            if 'DATE' in dataType.upper().strip():
                dateFormat = eval(sourceSchema.loc[sourceSchema['column_name'] == column, 'date_format'].item())
                detailedReport_DF = dataTypeValidation(spark,dataType, target_DF, column, sourceTableName, targetTableName, sourceMandatoryFields, sourcePrimaryKey, dateFormat)
                if detailedReport_DF.count() > 0:
                    detailedMetadataReport_DF = detailedMetadataReport_DF.unionByName(detailedReport_DF)
            else:
                detailedReport_DF = dataTypeValidation(spark,dataType, target_DF, column, sourceTableName, targetTableName, sourceMandatoryFields, sourcePrimaryKey, dateFormat=None)
                if detailedReport_DF.count() > 0:
                    detailedMetadataReport_DF = detailedMetadataReport_DF.unionByName(detailedReport_DF)

        sfOptions = {
        "sfURL" : "tlktcnk-kq18739.snowflakecomputing.com/",
        "sfUser" : "sushma",
        "sfPassword" : "Maveric@123",
        "sfDatabase" : "test_db",
        "sfSchema" : "public",
        "sfWarehouse" : "compute_wh",
        "application" : "AWSGlue"
        }
        detailedMetadataReport_DF.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "Failed_Quality_Check_Records_Source_Stage").mode("overwrite").save()
    
        target_DF.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "Passed_Quality_Check_Records_Source_Stage").mode("overwrite").save()
        s3Resource = boto3.resource('s3')
        bucket_name = 's3-bucket-hackathon'
        filekey = "NSE_BHAV_data/data/"
        bucketClient = s3Resource.Bucket(bucket_name)
        prefix_objs = bucketClient.objects.filter(Prefix=filekey)
        filesList = [obj.key for obj in prefix_objs if obj.key!=filekey]
        s3 = boto3.client('s3')
        newPrefix = "NSE_BHAV_data_archive/"
        for filePath in filesPath:
            fileName = filePath.split('/')[-1]
            old_source = { 'Bucket': bucket_name,
                           'Key': filePath}
            # replace the prefix
            s3.copy_object(Bucket = bucket_name, CopySource = old_source, Key = newPrefix +fileName)
            s3.delete_object(Bucket = bucket_name, Key = filePath)
        


job.commit()