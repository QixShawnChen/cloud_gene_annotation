# thaw.py
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
import re
import json
import time
from boto3.dynamodb.conditions import Key
from configparser import ConfigParser
import shutil

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
config = ConfigParser()
config.read('thaw_config.ini')

# AWS clients
sqs = boto3.client('sqs')
s3 = boto3.client('s3')
sns = boto3.client('sns')
glacier = boto3.client('glacier')
dynamodb = boto3.resource('dynamodb')

# Configuration parameters
cnet_id = config.get('info', 'cnet_id')
job_info_dir = config.get('paths', 'job_info_dir')
s3_results_bucket = config.get('aws', 's3_results_bucket')
queue_url_thaw = config.get('aws', 'queue_url_thaw')
dynamodb_table_name = config.get('aws', 'dynamodb_table_name')
glacier_vault = config.get('aws', 'glacier_arn')

table = dynamodb.Table(dynamodb_table_name)

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

def check_restore_status(glacier_vault, jobId):
    try:
        response = glacier.describe_job(vaultName=glacier_vault, jobId=jobId)
        return response['StatusCode']
    except Exception as e:
        print(f"Error checking restore status for job ID {jobId}: {e}")
        return None

def download_restored_file(glacier_vault, jobId, download_path, file_name):
    try:
        response = glacier.get_job_output(vaultName=glacier_vault, jobId=jobId)
        print(response)
        full_path = os.path.join(download_path, file_name)
        with open(full_path, 'wb') as f:
            f.write(response['body'].read())
        print(f"File downloaded to {full_path}")
        return full_path
    except Exception as e:
        print(f"Error downloading restored file: {e}")
        return None

def update_dynamodb_s3_restored(job_id, data):
    try:
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET s3_key_result_file = :result_key REMOVE archive_id REMOVE glacier_job_id',
            ExpressionAttributeValues={
                ':result_key': data['s3_key_result_file']
            }
        )
        print("DynamoDB updated successfully for restored s3_res_file.")
    except Exception as e:
        print(f"Error updating DynamoDB: {e}")

def delete_glacier_archive(glacier_vault, archive_id):
    try:
        glacier.delete_archive(vaultName=glacier_vault, archiveId=archive_id)
        print(f"Deleted archive ID {archive_id} from Glacier")
    except Exception as e:
        print(f"Error deleting archive ID {archive_id} from Glacier: {e}")

def delete_local_file(local_file_path):
    try:
        shutil.rmtree(local_file_path)
        print(f"Deleted local file {local_file_path}")
    except Exception as e:
        print(f"Failed to delete local file {local_file_path}: {str(e)}")

def main():
    while True:
        messages = sqs.receive_message(
            QueueUrl=queue_url_thaw,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20  # Use long polling
        )
        #print(messages)
        if 'Messages' in messages:
            for message in messages['Messages']:
                try:
                    body1 = json.loads(message['Body'])
                    body = json.loads(body1['Message'])
                    message_type = body['Action']
                    user_job_id = body['JobDescription']
                    matches = re.search(r"user_id: ([\w-]+), job_id: ([\w-]+)", user_job_id)
                    if matches:
                        user_id = matches.group(1)
                        job_id = matches.group(2)
                    else:
                        print("No job or user IDs found")
                    #print(user_id)
                    #print(message)
                    jobId = body['JobId']
                    archive_id = body['ArchiveId']
                    print(f'job_id: {job_id}')
                    print(f'message type: {message_type}')
                    print(f'glacier jobId: {jobId}')
                    print(f'archive_id: {archive_id}')
                    # 'Type': 'archive-retrieval', need a modification
                    if message_type == 'ArchiveRetrieval':
                        
                        status = check_restore_status(glacier_vault, jobId)
                        if status == 'Succeeded':
                            print(f"Restore complete for archive ID {archive_id}")
                            results_file_name = "restored_test.annot.vcf"

                            # Get the job_id from the archive_id
                            #job_id = get_job_id_for_user_and_archive(user_id, archive_id)
                            print(job_id)

                            # Download the restored file to S3 result bucket
                            s3_key_results_file = f"{cnet_id}/{user_id}/{job_id}/{results_file_name}"
                            print(s3_key_results_file)
                            local_file_path = os.path.join(job_info_dir, job_id)
                            local_file_path_mkdir = os.path.join(job_info_dir, job_id, results_file_name)
                            os.makedirs(os.path.dirname(local_file_path_mkdir), exist_ok=True)
                            print(f'local_file_path: {local_file_path}')
                            restored_file_path = download_restored_file(glacier_vault, jobId, local_file_path, results_file_name)

                            # Upload the restored file to S3
                            s3.upload_file(restored_file_path, s3_results_bucket, s3_key_results_file)

                            # Update the s3_key_result_file in DynamoDB
                            #data = {
                                #'s3_key_result_file': s3_key_results_file,
                            #}
                            #update_dynamodb_s3_restored(job_id, data)

                            # Delete the archive from Glacier
                            delete_glacier_archive(glacier_vault, archive_id)

                            # Update the job status to RESTORED and Update the s3_key_result_file in DynamoDB
                            table.update_item(
                                Key={'job_id': job_id},
                                ConditionExpression='job_status = :current_status',
                                UpdateExpression='SET job_status = :new_status, s3_key_result_file = :s3_key_results_file REMOVE archive_id',
                                ExpressionAttributeValues={
                                    ':s3_key_results_file': s3_key_results_file,
                                    ':current_status': 'RESTORING',
                                    ':new_status': 'RESTORED'
                                }
                            )

                            print("DynamoDB: JOB STATUS, s3_key_result_file, and archive_id updated to RESTORED successfully.")
                            path_to_del_local = local_file_path
                            delete_local_file(path_to_del_local)
                            print("Local restored file successfully deleted.")
                            sqs.delete_message(
                                QueueUrl=queue_url_thaw,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                            break
                        elif status == 'Failed':
                            print(f"Restore failed for archive ID {archive_id}")
                            sqs.delete_message(
                                QueueUrl=queue_url_thaw,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                        else:
                            print(f"Restore in progress for archive ID {archive_id}. Checking again in 5 minutes...")
                            time.sleep(300)  # Wait for 5 minutes before checking again

                        
                except Exception as e:
                    print(f"Error processing message: {e}")

if __name__ == "__main__":
    main()
### EOF