import sys
import logging
import json
import pymysql
import boto3
import time
import datetime
from datetime import datetime
import csv
from botocore.client import Config
from datetime import date




'''PARAMETERS'''

#DB connection settings
host_name = ""
user_name = ""
user_password = ""
database_name = ""

#Path to the temprorary storage of Lambda
Path = "/tmp/"

#Fields of the csv file that will be imported to Personalize must match the JSON schema
users_fields = []
items_fields = []
interactions_fields = []


#SQL queries
select_users = "" 
select_items = ""
select_interactions = ""

#S3 parameter
s3BucketName=""

# ARN ROLE used to create the import job
ARN_ROLE = "" 


#DataSetGroupName
DataSetGroupArn = ""


logger = logging.getLogger()

# define connection function and sql query function
def create_connection(host_name, user_name, user_password,database_name):
    connection = None
    logger.setLevel(logging.INFO)
    conn = pymysql.connect(host_name, user_name, user_password, database_name, connect_timeout=5)
    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")    
    return conn

def execute_read_query(connection, query):
     with connection.cursor() as cursor:
        cursor.execute(query)
        FinalResult = []
        for row in cursor:
            #print(row)
            FinalResult.append(row)
        #print(FinalResult)
        connection.close()
        return FinalResult



def lambda_handler(event, context):
    
    '''Query user's metadata in the SQL DB'''
    
    # DB connection
    connection = create_connection(host_name, user_name, user_password,database_name)
    # DB query
    Rows = execute_read_query(connection, select_users)
    #Write the csv file
    with open(Path + "users.csv", 'w',newline='') as f:
        writer = csv.writer(f)
        writer.writerows([tuple(JSON_fields)])
        writer.writerows(rows)

    
    
    # Query items's metadata
    
    # DB connection
    connection = create_connection(host_name, user_name, user_password,database_name)
    # DB query
    Rows = execute_read_query(connection, select_items)
    #Write the csv file
    with open(Path + "items.csv", 'w',newline='') as f:
        writer = csv.writer(f)
        writer.writerows([tuple(JSON_items)])
        writer.writerows(rows)



    # Query interactions 

    # DB connection
    connection = create_connection(host_name, user_name, user_password,database_name)
    # DB query
    Rows = execute_read_query(connection, select_interactions)   
    #Write the csv file
    with open(Path + "interactions.csv", 'w',newline='') as f:
        writer = csv.writer(f)
        writer.writerows([tuple(JSON_interactions)])
        writer.writerows(GlobalRow)
    
    



    '''Update the csv files to s3'''
    
    config = Config(connect_timeout=360, retries={'max_attempts': 3})
    s3 = boto3.resource('s3',config=config)
   

    try:
        data = open(Path + 'users.csv', 'rb')
        s3.Bucket(s3BucketName).put_object(Key='users.csv', Body=data)
    except Exception as e:
        print(e)
    
    try:
        data = open(Path + 'items.csv', 'rb')
        s3.Bucket(s3BucketName).put_object(Key='items.csv', Body=data)
    except Exception as e:
        print(e)
        
    try:
        data = open(Path + 'interactions.csv', 'rb')
        s3.Bucket(s3BucketName).put_object(Key='interactions.csv', Body=data)
    except Exception as e:
        print(e)
   



    '''Create the dataset import job'''

    personalize = boto3.client('personalize')
    


    # Select the datasets

    response = personalize.list_datasets( datasetGroupArn = DataSetGroupArn)
    for i in range(len(response["datasets"])):
        print(response["datasets"][i]["datasetArn"])

        
    DatasetInteractions = personalize.describe_dataset(datasetArn = response["datasets"][0]["datasetArn"] )["dataset"]
    DatasetUsers = personalize.describe_dataset(datasetArn = response["datasets"][1]["datasetArn"] )["dataset"]
    DatasetItems = personalize.describe_dataset(datasetArn = response["datasets"][2]["datasetArn"] )["dataset"]

    print()
    print('Dataset Arn users: ' + DatasetUsers['datasetArn'])
    print('Dataset Arn items: ' + DatasetItems['datasetArn'])
    print('Dataset Arn interactions: ' + DatasetInteractions['datasetArn'])





    
    ''' Import csv from s3 to Personalize with a dataset import job'''

    today = date.today()


    #users dataset

    response = personalize.create_dataset_import_job(
        jobName = 'UsersImportJob' + today.strftime("%d_%m_%Y_%H_%M_%S") ,
        datasetArn = DatasetUsers['datasetArn'],
        dataSource = {'dataLocation':'s3://' + s3BucketName + '/users.csv'},
        roleArn = ARN_ROLE)

    dsij_arn_Users = response['datasetImportJobArn']
    print ('Dataset Import Job arn: ' + dsij_arn_Users)

    descriptionUsers = personalize.describe_dataset_import_job(datasetImportJobArn = dsij_arn_Users)['datasetImportJob']

    print('Name: ' + descriptionUsers['jobName'])
    print('ARN: ' + descriptionUsers['datasetImportJobArn'])
    print('Status: ' + descriptionUsers['status'])





    #items dataset

    response = personalize.create_dataset_import_job(
        jobName = 'ItemsImportJob' + today.strftime("%d_%m_%Y_%H_%M_%S"),
        datasetArn = DatasetItems['datasetArn'],
        dataSource = {'dataLocation':'s3://' + s3BucketName + '/items.csv'},
        roleArn = ARN_ROLE)

    dsij_arn_Items = response['datasetImportJobArn']
    print ('Dataset Import Job arn: ' + dsij_arn_Items)

    descriptionItems = personalize.describe_dataset_import_job(datasetImportJobArn = dsij_arn_Items)['datasetImportJob']

    print('Name: ' + descriptionItems['jobName'])
    print('ARN: ' + descriptionItems['datasetImportJobArn'])
    print('Status: ' + descriptionItems['status'])






    #interactions dataset

    response = personalize.create_dataset_import_job(
        jobName = 'InteractionsImportJob' + today.strftime("%d_%m_%Y_%H_%M_%S"),
        datasetArn = DatasetInteractions['datasetArn'],
        dataSource = {'dataLocation':'s3://' + s3BucketName + '/interactions.csv'},
        roleArn = ARN_ROLE)

    dsij_arn_Interactions = response['datasetImportJobArn']

    print ('Dataset Import Job arn: ' + dsij_arn_Interactions)
    descriptionInteractions = personalize.describe_dataset_import_job(datasetImportJobArn = dsij_arn_Interactions)['datasetImportJob']

    print('Name: ' + descriptionInteractions['jobName'])
    print('ARN: ' + descriptionInteractions['datasetImportJobArn'])
    print('Status: ' + descriptionInteractions['status'])


    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Imported the data to Personalize')
    }
