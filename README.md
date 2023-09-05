# Maveric Hackathon 2023

# Data Pipeline and Test Automation 

#### Mentor: Sunil Prakash 

#### Team members 
  * Akheel Ahmed Siddiqui 
  * Amith Rakshan Suresh 
  * Janani R 
  * Mohith Dandu 
  * Sailesh Singh Chauhan 
  * Sushma N

## 💡 Problem Statement :
A characteristic of data pipeline development is the frequent release of high-quality data to gain user feedback and acceptance. At the end of every data pipeline iteration, it’s expected that the data is of high quality for the next phase. Automated testing is essential for the integration testing of data pipelines.

## 📝 Proposed solution :
This project is aimed at implementing a data pipeline and automated testing for a NSE Market and BHAV data. The objective is to ensure the accuracy, reliability, and consistency of the data being processed and to provide a seamless and efficient workflow for data handling to end user. 

## Architecture of Automated data pipeline with Data Validation
![Screenshot1](https://github.com/saileshchauhan/Hackathon2023/blob/master/Hackathon_DataPipeline-Final-2.png)


## 🛠 Components of Data Pipeline Framework with test automation :

The data pipeline is designed to handle the complete lifecycle of data processing. It encompasses the following key steps: 
* #### 📝 Data source description:
  From NSE server we are obtaining Market files which includes capital market transaction data for equities listed in NSE. (BHAV Historical data from 01-01-2016 ) 
* #### Data Ingestion(bronze layer):
   Fetches data from NSE server and save data in intermediate stage that is AWS S3 bucket 
* #### Data validation:
  Meta data validation is performed and obtain the data which has passed the quality check and Validates the source data against predefined criteria which is Meta data to ensure its integrity and consistency. 
  * #### Quality Check:
    comprises of null data validation, primary key validation, duplicate check, Data type an data length validation.
  * #### Error Handling:
    Tests the handling of erroneous scenarios, such as missing or inaccessible data sources, faulty transformations, or incomplete data storage by reporting the quality check failed records.
  * #### Data Storage(Silver layer):
    The validated data sent to Snowflake (Data warehouse) for further transformation.
  * #### Data Transformation(golden layer):
    Calculating spread high low, spread open close, Returns percentage.
  * #### Data Analysis:
    Analyzing the stored data to identify patterns, trends, and insights and building dashboard in Power BI.

## Data Pipeline Execution time from MKT to the S3 staging :
![image](https://github.com/saileshchauhan/Hackathon2023/assets/76868785/94fed1f0-b7ab-4585-be89-a3491e259a94)
![image](https://github.com/saileshchauhan/Hackathon2023/assets/76868785/5439c097-dca5-424d-916c-a7344008a7dc)
![image](https://github.com/saileshchauhan/Hackathon2023/assets/76868785/b33aaaad-90d9-45aa-9a50-387932aa4529)
![image](https://github.com/saileshchauhan/Hackathon2023/assets/76868785/7ae1460a-54b2-45b3-a035-06569631f9d1)
![image](https://github.com/saileshchauhan/Hackathon2023/assets/76868785/5612df37-a484-4ea8-b627-790f32f1554a)
![image](https://github.com/saileshchauhan/Hackathon2023/assets/76868785/5097f393-d79a-4d88-9c42-604081175678)




## Data Pipeline Execution time From BHAV data to the S3 staging :
![image](https://github.com/saileshchauhan/Hackathon2023/assets/76868785/196762c3-eacd-4881-957c-cebfa3083f34)

## 🛠 Tools and Technology utilised: 
* AWS S3, glue, workflows
* Snowflake for data warehousing 
* Pyspark is the programming language used for developing the code. 
* Visual studio code for development purpose. 
* GitHub is used as a version control system.
* Power BI for Visualization.


