import boto3
import json
#from botocore.exceptions import ClientError
import time
import datetime
from datetime import date

from Extract_script import *

# #connecting to dynamo db
# client = boto3.client('dynamodb',
#                       aws_access_key_id=access_key_id,
#                       aws_secret_access_key=secret_access_key)
# print(client)

#creating an instance to connect with aws resource
#dynamodb = boto3.resource('dynamodb')

def sns_setup():
    sns = boto3.client('sns')
    etl_topic_name = 'Event-driven-etl'
    topics_list = sns.list_topics()
    topic_created_ind = False
    #verifying whether topic exists
    for topics in topics_list['Topics']:
        if topics['TopicArn'].endswith(etl_topic_name):
            print('topic alreadye exists')
            topic_created_ind = True

    #actual call to aws to create a topic
    if not topic_created_ind:
        topic_response = sns.create_topic(Name=etl_topic_name)
        topic_arn = topic_response['TopicArn']
        print(f'Topic Arn is {topic_arn}')
        if topic_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            #subscribe to topic, Enter phone number
            phonenumber =''
            subscribe_response = sns.subscribe(TopicArn=topic_arn, Protocol='sms', Endpoint=phonenumber)
            print(f'Subscription response is {subscribe_response}')
            # Send a SMS message to the specified phone number
            publish_response = sns.publish(
                TopicArn =topic_arn,
                Message='Topic Created',
            )
            print(f'Publish response is {publish_response}')


def send_notification(message_body):
    print('Enterning send Notification')
    message = message_body
    sns = boto3.client('sns')
    etl_topic_name = 'Event-driven-etl'
    topics_list = sns.list_topics()
    for topics in topics_list['Topics']:
        if topics['TopicArn'].endswith(etl_topic_name):
            topic_arn = topics['TopicArn']

    publish_response = sns.publish(
        TopicArn=topic_arn,
        Message=message,
    )
    print('Exiting send Notification')

def create_table(table_name):
    dynamodb = boto3.resource('dynamodb')
    print('In create Table method')
    # Create the DynamoDB table.
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'recorded_date',
                'KeyType': 'HASH'
            }
            # ,
            # {
            #     'AttributeName': 'cases',
            #     'KeyType': 'RANGE'
            # }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'recorded_date',
                'AttributeType': 'S'
            }
            # ,
            # {
            #     'AttributeName': 'cases',
            #     'AttributeType': 'S'
            # }

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    print(f'Current row count in table :{table_name} is {table.item_count}')
    print('Creating rundate table')
    dynamodb_client  = boto3.client('dynamodb')
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if 'rundate' not in existing_tables:
        table = dynamodb.create_table(
            TableName='rundate',
            KeySchema=[
                {
                    'AttributeName': 'last_run_date',
                    'KeyType': 'HASH'
                }

            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'last_run_date',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
    table.meta.client.get_waiter('table_exists').wait(TableName='rundate')
    print(f'Current row count in table : rundate is {table.item_count}')
    #create notification channel
    sns_setup()


def load_table(data):
    dynamodb = boto3.resource('dynamodb')
    table_name ='covid_data'
    table = dynamodb.Table(table_name)
    print(table)
    print(f'data is {data}')
    last_run_date = '1900-01-01'
    today_date = date.today().strftime('%Y-%m-%d')

    #create an instance to interact with rundate
    table2 = dynamodb.Table('rundate')
    # logic to handle re-running failed job to update rundate table
    rundate_data_old = get_item('rundate')

    for item in rundate_data_old:
        if today_date == item['last_run_date']:
            table.delete_item(Key={
                'last_run_date': item['last_run_date'],
            })

    # logic to delete the covid_data to reload current date data
    covid_data_old = get_item(table_name)
    for item in covid_data_old:
        if today_date == item['recorded_date']:
            table.delete_item(Key={
                    'recorded_date': today_date,
                })

    for item in data:
        date_timestamp = str(item[0]).split()[0]
        cases = str(item[1])
        deaths = str(item[2])
        recovered = str(int(item[3]))
        print ( f"date:{date_timestamp}, cases: {cases}, deaths:{deaths},recoveries:{recovered}")
        #write this message to dynamo DB
        response = table.put_item(
            Item={
                'recorded_date': date_timestamp,
                'cases': cases,
                'deaths': deaths ,
                'recoveries' : recovered
            }

        )
        last_run_date = date_timestamp
        if (last_run_date <= date_timestamp):
            last_run_date = date_timestamp
        print(response)
    # with open('run_date.txt','w') as f:
    #     f.write(last_run_date)
    #     pass

    print(f'last run date is {last_run_date}')


    #reading covid_data table to get latest row count
    row_count = len(table.scan()['Items'])
    response_rundate = table2.put_item(
        Item={
            'last_run_date':last_run_date,
            'row_count': row_count
        }
    )
    print(f'Response from run date insert is {response_rundate}')


def copy_to_s3_csv(bucket_name):

    # insert data into csv file for dashboard
    s3_client = boto3.client('s3')
    covid_data = get_item(table_name='covid_data')
    print('****************************************')
    print(covid_data)
    data_csv_format = []
    for data_item in covid_data:
        data_csv_format.append(
        f"{data_item['recorded_date']},{data_item['recoveries']},{data_item['cases']},{data_item['deaths']}")
        # temp file name
        file_path = '/tmp/' + 'covid_data.csv'
        with open(file_path, 'w') as f:
            f.write('recorded_date,recoveries,cases,deaths \n')
            for data_item in data_csv_format:
                f.write(data_item + '\n')
        # copy the file to s3
        s3_response = s3_client.upload_file(file_path, bucket_name, 'covid_data.csv')
        print(s3_response)


def get_item(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    try:
        # response = table.query(
        # FilterExpression="#d = :date",
        # ExpressionAttributeName ="{'#d':'date'}",
        # ExpressionAttributeValues ={
        #    ":date": {'S':'2020-09-01'}
        # }
        response = table.scan()
        print(type(response['Items']))
        print(response['Items'])

        #print(f'response is {response}')
    except Exception as e:
        print(e.response['Error']['Message'])
    else:
        return response['Items']



#if __name__== '__main__' :
def lambda_handler(event, context):
    try:
        dynamodb = boto3.client('dynamodb')
        #table name
        table_name = 'covid_data'
        existing_tables = dynamodb.list_tables()['TableNames']
        print(existing_tables)
        full_load = False
        us_url ="https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
        john_hopkins_data_url = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1597116776410r0.9370121973993177"
        if table_name not in existing_tables:
            create_table(table_name)
            full_load = True


        if full_load:

            data = extract(us_url,john_hopkins_data_url)
            print(f'data type of extract data is {type(data)}')
            list_data = []
            for item in iter(data.values):
                print(item)
                list_data.append(item)
                print('*****************')
                print(list_data)

            #Initial load to DB
            load_table(list_data)
        else:
            # get last run date from rundate table
            rundate_data = get_item('rundate')
            print(f'rundate data is {rundate_data}')
            last_run_date = ''
            if not rundate_data == []:
                last_run_date = rundate_data[0]['last_run_date']
                last_row_count = rundate_data[0]['row_count']
                print(f'Rundate is {last_run_date}, row count is {last_row_count}')
            #Get Data from transform
            data_df = extract(us_url,john_hopkins_data_url)
            if not last_run_date == '':
                data_df = data_df[(data_df['date']> last_run_date)]
            print(f'data type of extract data is {type(data_df)}')

            list_data = []
            for item in iter(data_df.values):
                print(item)
                list_data.append(item)
                print('*****************')
                print(list_data)
            load_table(list_data)
        #insert data into s3 for building dashboards
        s3_client = boto3.client('s3')
        bucket_list = s3_client.list_buckets()
        bucket_name = 'python-code-acloudguru-challenge'
        bucket_list = bucket_list['Buckets']
        print(bucket_list)
        bucket_name_list = []
        if len(bucket_list) > 0:
            for item in bucket_list:
                bucket_name_list.append(item['Name'])
                # item['Name'])
            if bucket_name in bucket_name_list:
                print(f'{bucket_name} is available')
                copy_to_s3_csv(bucket_name)
            else:
                response = None
                response = s3_client.create_bucket(Bucket=bucket_name)
                print(f'{response}')
                copy_to_s3_csv(bucket_name)
            print(bucket_name_list)
        # Success Notification
        send_notification('ETL process completed successfully')
        return "Event Driven ETL Processed succesfully"
    except Exception as e:
        print('In exception block')
        print(e)
        send_notification('ETL failed. Please take a look')
        #return "Event Driven ETL Processed Failed"
