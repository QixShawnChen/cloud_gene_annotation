from flask import Flask, request, jsonify, render_template, url_for
import subprocess
import uuid
import os
import json
import boto3
import shutil
from datetime import datetime
from botocore.exceptions import NoCredentialsError, ClientError
from decimal import Decimal
from configparser import ConfigParser

# Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

# Get configuration
config = ConfigParser()
config.read('ann_config.ini')

# AWS clients
sqs = boto3.client('sqs')
s3 = boto3.client('s3')
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

s3_results_bucket = config.get('aws', 's3_results_bucket')
queue_url_requests = config.get('aws', 'queue_url_requests')
queue_url_results = config.get('aws', 'queue_url_results')
queue_url_archive = config.get('aws', 'queue_url_archive')
topic_arn_requests = config.get('aws', 'topic_arn_requests')
topic_arn_results = config.get('aws', 'topic_arn_results')
topic_arn_archive = config.get('aws', 'topic_arn_archive')
dynamodb_table_name = config.get('aws', 'dynamodb_table_name')
input_file_path = config.get('paths', 'input_file_path')
job_info_dir = config.get('paths', 'job_info_dir')

table = dynamodb.Table(dynamodb_table_name)

def download_file_from_s3(bucket_name, s3_key, local_file_path):
    try:
        s3.download_file(bucket_name, s3_key, local_file_path)
        return True
    except Exception as e:
        print(f"Failed to download file from S3: {str(e)}")
        return False

# Poll the message queue in a loop using long polling
while True:
    # Attempt to read a message from the queue
    messages = sqs.receive_message(
        QueueUrl=queue_url_requests,
        AttributeNames=['All'],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20  # Use long polling
    )

    if 'Messages' in messages:
        for message in messages['Messages']:
            # Extract job parameters from the message body
            body1 = json.loads(message['Body'])
            body = json.loads(body1['Message'])
            user_id = body['user_id']
            job_id = body['job_id']
            input_file_name = body['input_file_name']
            s3_inputs_bucket = body['s3_inputs_bucket']
            s3_key_input_file = body['s3_key_input_file']

            # Directory structure for job files
            local_file_path = os.path.join(job_info_dir, job_id, os.path.basename(s3_key_input_file))
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            # Download file from S3
            try:
                s3.download_file(s3_inputs_bucket, s3_key_input_file, local_file_path)
            except Exception as e:
                print(f"Failed to download file from S3: {str(e)}")
                continue

            # Check the current job status
            try:
                response = table.get_item(
                    Key={'job_id': job_id}
                )
                item = response.get('Item')
                current_status = item.get('job_status', 'UNKNOWN')
                print(f"Current status for job_id {job_id}: {current_status}")
            except Exception as e:
                print(f"Failed to get job status from DynamoDB: {str(e)}")
                continue

            if current_status == 'PENDING':
                # Update job status in DynamoDB
                try:
                    table.update_item(
                        Key={'job_id': job_id},
                        UpdateExpression='SET job_status = :new_status, user_id = :user',
                        ConditionExpression='job_status = :current_status',
                        ExpressionAttributeValues={
                            ':new_status': 'RUNNING',
                            ':user': user_id,
                            ':current_status': 'PENDING'
                        }
                    )
                except Exception as e:
                    print(f"Failed to update job status in DynamoDB: {str(e)}")
                    continue

                # Launch annotation job as a background process
                try:
                    command = ['python', './run.py', local_file_path, user_id]
                    job = subprocess.Popen(command)
                    print("Successfully Started Popen")

                    # Delete the message from the queue if job was successfully submitted
                    sqs.delete_message(
                        QueueUrl=queue_url_requests,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print(f"Job {job_id} started successfully.")
                except Exception as e:
                    print(f"Failed to start job {job_id}: {str(e)}")
            else:
                print(f"Job {job_id} is not in PENDING state, skipping.")
    #else:
        #print("No messages received")


# Load configuration
'''config = ConfigParser()
config.read('ann_config.ini')

#reference: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

# reference: https://docs.aws.amazon.com/dynamodb/


# Access values from the config file
# AWS clients
sqs = boto3.client('sqs')
s3 = boto3.client('s3')
sns = boto3.client('sns')
s3_results_bucket = config.get('aws', 's3_results_bucket')
queue_url_requests = config.get('aws', 'queue_url_requests')
queue_url_results = config.get('aws', 'queue_url_results')
queue_url_archive = config.get('aws', 'queue_url_archive')
topic_arn_requests = config.get('aws', 'topic_arn_requests')
topic_arn_results = config.get('aws', 'topic_arn_results')
topic_arn_archive = config.get('aws', 'topic_arn_archive')
dynamodb_table_name = config.get('aws', 'dynamodb_table_name')
input_file_path = config.get('paths', 'input_file_path')
job_info_dir = config.get('paths', 'job_info_dir')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodb_table_name)




def download_file_from_s3(bucket_name, s3_key, local_file_path):
    try:
        s3.download_file(bucket_name, s3_key, local_file_path)
        return True
    except Exception as e:
        print(f"Failed to download file from S3: {str(e)}")
        return False
    




# Poll the message queue in a loop using long polling
while True:
    # Attempt to read a message from the queue
    messages = sqs.receive_message(
        QueueUrl = queue_url_requests,
        AttributeNames = ['All'],
        MaxNumberOfMessages = 1,
        WaitTimeSeconds = 20  # Use long polling
    )

    if 'Messages' in messages:
        for message in messages['Messages']:
            # Extract job parameters from the message body
            body1 = json.loads(message['Body'])
            body = json.loads(body1['Message'])
            user_id = body['user_id']
            job_id = body['job_id']
            input_file_name = body['input_file_name']
            s3_inputs_bucket = body['s3_inputs_bucket']
            s3_key_input_file = body['s3_key_input_file']

            # Directory structure for job files
            local_file_path = os.path.join(job_info_dir, job_id, os.path.basename(s3_key_input_file))
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            # Examing
            #print(local_file_path)

            # Download file from S3
            try:
                s3.download_file(s3_inputs_bucket, s3_key_input_file, local_file_path)
            except Exception as e:
                print(f"Failed to download file from S3: {str(e)}")
                continue

            # Update job status in DynamoDB
            try:
                table.update_item(
                    Key = {'job_id': job_id},
                    UpdateExpression = 'SET job_status = :new_status, user_id = :user',
                    ConditionExpression='job_status = :current_status',
                    ExpressionAttributeValues = {
                        ':new_status': 'RUNNING',
                        ':user': user_id,
                        ':current_status': 'PENDING'
                    }
                )
            except Exception as e:
                print(f"Failed to update job status in DynamoDB: {str(e)}")
                continue

            # Launch annotation job as a background process
            try:
                command = ['python', './run.py', local_file_path]
                job = subprocess.Popen(command)

                # Delete the message from the queue if job was successfully submitted
                sqs.delete_message(
                    queue_url_requests=queue_url_requests,
                    ReceiptHandle=message['ReceiptHandle']
                )
                print(f"Job {job_id} started successfully.")
            except Exception as e:
                print(f"Failed to start job {job_id}: {str(e)}")




'''