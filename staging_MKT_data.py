import sys
import boto3
import os
import paramiko
import configparser
import shutil
import gzip
from struct import *
import json
import tempfile
from awsglue.utils import getResolvedOptions
from time import sleep
from datetime import datetime, timedelta
from io import BytesIO, StringIO
import csv
import time

args = getResolvedOptions(sys.argv, ['start_date', 'end_date', 'region', 'config_path', 's3_bucket', 'rsa_key_secret_name', 'passphrase_secret_name', 'data_path'])

start_date = args['start_date']
end_date = args['end_date']
region = args['region']
config_path = args['config_path']
s3_bucket = args['s3_bucket']
rsa_key_secret_name = args['rsa_key_secret_name']
passphrase_secret_name = args['passphrase_secret_name']
data_path = args['data_path']

s3 = boto3.resource('s3')
bucket = s3.Bucket(s3_bucket)
s3_client = boto3.client('s3')

secrets = boto3.client(service_name='secretsmanager', region_name=region)

tmp_dir = tempfile.mkdtemp()

config = configparser.ConfigParser()
configpath = config_path.replace("s3://","").split("/",1)[1]
config_object = s3.Object(s3_bucket, configpath)
config_object_content = config_object.get()['Body'].read().decode('utf-8')
config.read_string(config_object_content)

RemoteServerIP = config.get('NSE-SFTP-credentials', 'RemoteServerIP')
User = config.get('NSE-SFTP-credentials', 'User')
PortNumber = config.get('NSE-SFTP-credentials', 'PortNumber')
RemoteFileDirectory = config.get('NSE-SFTP-credentials', 'RemoteFileDirectory')

rsa_key_response = secrets.get_secret_value(SecretId=rsa_key_secret_name)
passphrase_response = secrets.get_secret_value(SecretId=passphrase_secret_name)
rsa_key_encrypted = rsa_key_response['SecretString']
passphrase = json.loads(passphrase_response['SecretString'])["NSE_SFTP_rsa_private_key_passphrase"]
key = paramiko.RSAKey(file_obj = StringIO(rsa_key_encrypted), password=passphrase)

transport = paramiko.Transport((RemoteServerIP, int(PortNumber)))
transport.connect(None, username=User, pkey=key)
sftp = paramiko.SFTPClient.from_transport(transport)
print("Connection successfully established ... ")

date_list = sftp.listdir(path=RemoteFileDirectory)
print("Files are available for the dates: ", date_list)

print("Start date provided is: ", start_date)
print("End date provided is: ", end_date)

start = datetime.strptime(start_date, "%B%d%Y")
end = datetime.strptime(end_date, "%B%d%Y")

date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days+1)]

t_time = time.time()

for date in date_generated:
    start_time = time.time()
    
    date = date.strftime("%B%d%Y")
    
    if date in date_list:
        RemLoc = RemoteFileDirectory + date
        print("Remote directory is: ", RemLoc)
        
        file_list = sftp.listdir(RemLoc)
        print("Total number of files in remote directory for the date are: ", len(file_list))

        record_counter = 0
        file_counter = 0
        
        mkt_list = []
        for file in file_list:
            if ".mkt.gz" in file:
                mkt_list.append(file)
        print("Total number of market files in the remote directory are: ", len(mkt_list))
        
        try:
            for i in range(len(mkt_list)):
                sftp.get(RemLoc + '/' + mkt_list[i], os.path.join(tmp_dir, mkt_list[i]))
                dict_list=[]
                with gzip.open(os.path.join(tmp_dir, mkt_list[i]), 'rb') as file_object:
                    while True:
                        row_data = {}
                        data = file_object.read(78)
                        if len(data) == 78:
                            unpacked_data = unpack('=HL2H13L16x', data)
            
                            row_data["Transcode"] = unpacked_data[0]
                            row_data["Timestamp"] = unpacked_data[1]
                            row_data["Message_Length"] = unpacked_data[2]
                            row_data["Security_Token"] = unpacked_data[3]
                            row_data["Last_Traded_Price"] = unpacked_data[4]
                            row_data["Best_Buy_Quantity"] = unpacked_data[5]
                            row_data["Best_Buy_Price"] = unpacked_data[6]
                            row_data["Best_Sell_Quantity"] = unpacked_data[7]
                            row_data["Best_Sell_Price"] = unpacked_data[8]
                            row_data["Total_Traded_Quantity"] = unpacked_data[9]
                            row_data["Average_Traded_Price"] = unpacked_data[10]
                            row_data["Interval_Open_Price"] = unpacked_data[11]
                            row_data["Interval_High_Price"] = unpacked_data[12]
                            row_data["Interval_Low_Price"] = unpacked_data[13]
                            row_data["Interval_Close_Price"] = unpacked_data[14]
                            row_data["Interval_Total_Traded_Quantity"] = unpacked_data[15]
                            
                            dict_list.append(row_data)
                            record_counter += 1
                            
                        else:
                            break
            
                if len(dict_list) > 0:
                    csv_data = ''
                    csv_data += ','.join(dict_list[0].keys()) + '\n'  # Header row
                    for row in dict_list:
                        csv_data += ','.join(map(str, row.values())) + '\n'
                    s3_client.put_object(Bucket=s3_bucket, Key=f'{data_path}/{date}/{mkt_list[i][:-3]}.csv', Body=csv_data)
                    file_counter += 1
                else:
                    print(f"File {mkt_list[i]} is empty. Moving on...")
        except Exception as e:
                print(e)
        
        print(f"All the files for the date: {date} are staged successfully.\nTotal number of files saved are: {file_counter}.\nTotal number of records for the day are: {record_counter}")
        print(f"Time taken to download, process and stage the data in S3 for the date: {date} is {time.time() - start_time}s.")
    else:
        print("Opps!! Files are not available for the date: ", date)
        continue

print("Deleting market files from temporary location........")

shutil.rmtree(tmp_dir)

print(f"Total time taken to stage the data to S3 is: {time.time() - t_time}s.")

print("Execution completed!!!")