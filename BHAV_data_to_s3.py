import sys
import boto3
from awsglue.utils import getResolvedOptions
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from io import BytesIO
import zipfile
import time

args = getResolvedOptions(sys.argv, ['start_date', 'end_date', 's3_file_path'])

start_date = args['start_date']
end_date = args['end_date']
s3_file_path = args['s3_file_path']

s3 = boto3.client('s3')
bucket_name = s3_file_path.replace("s3://","").split("/")[0]
data_path = s3_file_path.replace("s3://","").split("/",1)[1]

def send_to_s3(csv_data,file_name):
    s3.put_object(Body=csv_data, Bucket=bucket_name, Key=data_path + file_name)
    records_count = csv_data.decode().count('\n')-1
    print("-"*75)
    print(f"File: {file_name} sent to s3 bucket. Total number of records in the file are: {records_count}")
    return records_count

def scrape(link,year,day,month):
    startTime=time.time()
    try:
        req = Request(link)
        request = urlopen(req, timeout=5)
        status_code = request.getcode()
        if status_code==200:
            zip_file = zipfile.ZipFile(BytesIO(request.read()))
            names = zip_file.namelist()
            records_count=0
            for name in names:
                csv_data=zip_file.open(name).read()
                records_count=records_count+send_to_s3(csv_data,name)
                endTime=time.time()
                timeDiff=endTime-startTime
                print("Time taken to send the file to s3 is:",timeDiff,"seconds")
                print("-"*75)
            return records_count,status_code
    except Exception as e:
        # print(e)
        print("{}-{}-{} is NSE holiday".format(year, month, day))
        return 0,0

def main():
    start = datetime.strptime(str(start_date), "%Y-%m-%d")
    end = datetime.strptime(str(end_date), "%Y-%m-%d")
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days+1)]
    sTime=time.time()

    total_records_count=0
    file_count=0
    for date in date_generated:
        day=date.strftime("%d")
        month=(date.strftime("%B")[:3]).upper()
        year=date.strftime("%Y")

        fname="cm"+str(day)+str(month)+str(year)+"bhav.csv.zip"
        link="https://archives.nseindia.com/content/historical/EQUITIES/"+str(year)+"/"+month+"/"+fname
        records_count,status_code=scrape(link,year,day,month)
        if status_code==200:
            total_records_count=total_records_count+records_count
            file_count=file_count+1
    eTime=time.time()
    print("*"*75)
    print("The total number of files scraped from the website are:",file_count)
    print("*"*75)
    print("The total number of records sent to s3 bucket are:",total_records_count)
    print("*"*75)
    print("The total time taken to send the files to s3 bucket is:",eTime-sTime,"seconds")
    print("*"*75)
    print("Execution completed!!!")

main()