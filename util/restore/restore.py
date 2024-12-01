# restore.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

# Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

import os
import sys
import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
from configparser import ConfigParser

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
config = ConfigParser()
config.read('restore_config.ini')

# AWS clients
sqs = boto3.client('sqs')
sns = boto3.client('sns')
glacier = boto3.client('glacier')
dynamodb = boto3.resource('dynamodb')

# Configuration parameters
s3_results_bucket = config.get('aws', 's3_results_bucket')
queue_url_restore = config.get('aws', 'queue_url_restore')
topic_arn_thaw = config.get('aws', 'topic_arn_thaw')
dynamodb_table_name = config.get('aws', 'dynamodb_table_name')
glacier_vault = config.get('aws', 'glacier_arn')

table = dynamodb.Table(dynamodb_table_name)

def get_archive_ids_for_user(user_id, table):
    try:
        response = table.query(
            IndexName='user_id-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('user_id').eq(user_id),
            FilterExpression=Attr('job_status').eq('ARCHIVED')
        )
        items = response.get('Items', [])
        archive_ids = [item['archive_id'] for item in items if 'archive_id' in item]
        return archive_ids
    except Exception as e:
        print(f"Error querying DynamoDB: {e}")
        return []

def initiate_restore(glacier_vault, archive_id, user_id, job_id, days=1, retry_interval = 10):
    flag = True
    # In case it fails to work, we try it 10 times
    trial_left = 10
    while flag and trial_left >= 0:
        try:
            response = glacier.initiate_job(
                vaultName=glacier_vault,
                jobParameters={
                    'Type': 'archive-retrieval',
                    'ArchiveId': archive_id,
                    'Description': f"user_id: {user_id}, job_id: {job_id}",
                    'Tier': 'Standard',
                    'SNSTopic': topic_arn_thaw
                }
            )
            jobId = response['jobId']
            print(f"Restore initiated for archive ID: {archive_id}, glacier job ID: {jobId}")
            flag = False
            print("Successfully Started Restoring")
            print(f"Restore initiated for archive ID: {archive_id} using Expedited tier, glacier job ID: {jobId}")
            return jobId
        except Exception as e:
            trial_left -= 1
            print(f"Error initiating restore for archive ID {archive_id}: {e}")
            print(f"Going to try again after 10 sec. Trial left: {trial_left}")
            time.sleep(retry_interval)


import time

def initiate_restore_gao_su(glacier_vault, archive_id, user_id, job_id, days=1, retry_interval=10):
    flag = True
    while flag:
        try:
            response = glacier.initiate_job(
                vaultName=glacier_vault,
                jobParameters={
                    'Type': 'archive-retrieval',
                    'ArchiveId': archive_id,
                    'Description': f"user_id: {user_id}, job_id: {job_id}",
                    'Tier': 'Expedited',
                    'SNSTopic': topic_arn_thaw
                }
            )
            flag = False
            jobId = response['jobId']
            print("Successfully Shang Gao Su")
            print(f"Restore initiated for archive ID: {archive_id} using Expedited tier, glacier job ID: {jobId}")
            return jobId
        except:
            time.sleep(retry_interval)

    
    

def get_job_id_for_user_and_archive(user_id, archive_id):
    try:
        response = table.query(
            IndexName='user_id-index',
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        items = response.get('Items', [])
        for item in items:
            if item.get('archive_id') == archive_id:
                return item.get('job_id')
        
        print("No matching job_id found for the given user_id and archive_id.")
        return None
    except Exception as e:
        print(f"Error querying DynamoDB: {e}")
        return None


def main():
    while True:
        messages = sqs.receive_message(
            QueueUrl=queue_url_restore,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20  # Use long polling
        )
        if 'Messages' in messages:
            for message in messages['Messages']:
                try:
                    body1 = json.loads(message['Body'])
                    body = json.loads(body1['Message'])
                    message_type = body['message_type']
                    user_id = body['user_id']
                    
                    if message_type == 'restore_message':
                        archive_ids = get_archive_ids_for_user(user_id, table)
                        arc_jobIds = []
                        for archive_id in archive_ids:
                            job_id = get_job_id_for_user_and_archive(user_id, archive_id)
                            print(job_id)
                            table.update_item(
                                Key = {'job_id': job_id},
                                UpdateExpression = 'SET job_status = :new_status',
                                ConditionExpression='job_status = :current_status',
                                ExpressionAttributeValues = {
                                    ':new_status': 'RESTORING',
                                    ':current_status': 'ARCHIVED'
                                }
                            )

                            print("DynamoDB: JOB STATUS updated to RESTORING successfully.")
                            #jobId = initiate_restore(glacier_vault, archive_id, user_id, job_id)
                            #jobId = initiate_restore_gao_su(glacier_vault, archive_id, user_id, job_id)
                            jobId = initiate_restore(glacier_vault, archive_id, user_id, job_id)
                            archive_ids.remove(archive_id)
                            if jobId:
                                arc_jobIds.append((archive_id, jobId))
                        print(arc_jobIds)

                        # Send job ids to thaw.py via SNS
                        #message_thaw = {'message_type': 'thaw_message', 'user_id': user_id, 'arc_jobIds': arc_jobIds}
                        #try:
                            #response = sns.publish(
                                #TopicArn=topic_arn_thaw,
                                #Message=json.dumps({'default': json.dumps(message_thaw)}),
                                #Subject=f'Restore Initiation Notification: {topic_arn_thaw}',
                                #MessageStructure='json'
                            #)
                            #print(f"Notification to thaw.py sent successfully. Message ID: {response['MessageId']}")
                        #except Exception as e:
                            #print(f"Failed to send notification: {str(e)}")

                        sqs.delete_message(
                            QueueUrl=queue_url_restore,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                except Exception as e:
                    print(f"Error processing message: {e}")

if __name__ == "__main__":
    main()
### EOF