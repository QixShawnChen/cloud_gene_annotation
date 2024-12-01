# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import json
from botocore import exceptions

# Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
from helpers import get_user_profile

# Get configuration
from configparser import ConfigParser
config = ConfigParser()
config.read('archive_config.ini')

# AWS clients
sqs = boto3.client('sqs')
s3 = boto3.client('s3')
sns = boto3.client('sns')
glacier = boto3.client('glacier')
dynamodb = boto3.resource('dynamodb')

s3_results_bucket = config.get('aws', 's3_results_bucket')
queue_url_archive = config.get('aws', 'queue_url_archive')
topic_arn_archive = config.get('aws', 'topic_arn_archive')
queue_url_thaw = config.get('aws', 'queue_url_thaw')
topic_arn_thaw = config.get('aws', 'topic_arn_thaw')
dynamodb_table_name = config.get('aws', 'dynamodb_table_name')
input_file_path = config.get('paths', 'input_file_path')
job_info_dir = config.get('paths', 'job_info_dir')

table = dynamodb.Table(dynamodb_table_name)
glacier_arn = config.get('aws', 'glacier_arn')

def publish_sns_message(topic_arn, message):
    try:
        response = sns.publish(
            TopicArn=topic_arn,
            Message=json.dumps({'default': json.dumps(message)}),
            Subject=f'Completion Notification: {topic_arn}',
            MessageStructure='json'
        )
        print(f"Notification sent successfully. Message ID: {response['MessageId']}")
    except Exception as e:
        print(f"Failed to send notification: {str(e)}")

def main():
    while True:
        messages = sqs.receive_message(
            QueueUrl=queue_url_archive,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20  # Use long polling
        )
        if 'Messages' in messages:
            for message in messages['Messages']:
                # Extract job parameters from the message body
                body1 = json.loads(message['Body'])
                body = json.loads(body1['Message'])
                message_type = body['message_type']
                job_id = body['job_id']
                user_id = body['user_id']
                user_profile = get_user_profile(id=user_id)
                print(user_profile)
                user_status = user_profile[4]
                print(user_status)
                if message_type == 'archive_message' and user_status == 'free_user':
                #if message_type == 'archive_message':
                    response = table.get_item(
                        Key={'job_id': job_id}
                    )
                    item = response.get('Item')
                    if item:
                        try:
                            results_bucket = item['s3_results_bucket']
                            key_res_file = item['s3_key_result_file']
                            job_id = item['job_id']
                            user_id = item['user_id']
                            print(results_bucket)
                            print(key_res_file)
                            s3_response = s3.get_object(Bucket=results_bucket, Key=key_res_file)
                            file_content = s3_response['Body'].read()
                        except KeyError:
                            continue

                        glacier_response = glacier.upload_archive(
                            vaultName=glacier_arn,
                            body=file_content)
                        location = glacier_response['location']
                        archive_id = glacier_response['archiveId']
                        print(location)
                        print(archive_id)
                        try:
                            table.update_item(
                                Key={'job_id': job_id},
                                UpdateExpression='SET archive_id = :archive_id, job_status = :new_status REMOVE s3_key_result_file',
                                ConditionExpression='job_status = :current_status',
                                ExpressionAttributeValues={
                                    ':archive_id': archive_id,
                                    ':current_status': 'COMPLETED',
                                    ':new_status': 'ARCHIVED'
                                }
                            )
                        except Exception as e:
                            print(f"Failed to update archive id in DynamoDB: {str(e)}")
                            continue

                        try:
                            s3.delete_object(
                                Bucket=results_bucket,
                                Key=key_res_file)
                        except exceptions.ClientError as e:
                            print("Failed to delete the corresponding result file in S3 result bucket")
                            continue

                        # Deleting the message from the archive queue
                        try:
                            sqs.delete_message(
                                QueueUrl=queue_url_archive,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                        except Exception as e:
                            print(f"Failed to delete the message from the queue: {str(e)}")
                            continue

                        #message_thaw = {
                            #'message_type': 'thaw_message',
                            #'user_id': user_id
                        #}

                        #publish_sns_message(topic_arn_thaw, message_thaw)

                    else:
                        print(f"No item found in DynamoDB for job_id: {job_id}")
                elif message_type == 'archive_message' and user_status != 'free_user':
                    try:
                        sqs.delete_message(
                            QueueUrl=queue_url_archive,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print("Premium User doesn't need archive")
                    except Exception as e:
                        print(f"Failed to delete the message from the queue: {str(e)}")
                        continue
                    

        #else:
            #print("No messages received")

if __name__ == "__main__":
    main()


### EOF