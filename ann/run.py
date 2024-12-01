# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

# Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

import sys
import time
import driver
import boto3
import os
import shutil
import json
from datetime import datetime, timezone
from configparser import ConfigParser

#reference: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html



"""A rudimentary timer for coarse-grained profiling
"""
def upload_file_to_s3(bucket_name, s3_key, local_file_path):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"File {local_file_path} uploaded successfully to {bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload file to S3: {str(e)}")


def delete_local_file(local_file_path):
    try:
        shutil.rmtree(local_file_path)
        print(f"Deleted local file {local_file_path}")
    except Exception as e:
        print(f"Failed to delete local file {local_file_path}: {str(e)}")


def update_dynamodb(job_id, data):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('qixshawnchen_annotations')
    table.update_item(
        Key={'job_id': job_id},
        UpdateExpression='SET s3_results_bucket = :bucket, s3_key_result_file = :result_key, s3_key_log_file = :log_key, complete_time = :complete, job_status = :status',
        ExpressionAttributeValues={
            ':bucket': data['s3_results_bucket'],
            ':result_key': data['s3_key_result_file'],
            ':log_key': data['s3_key_log_file'],
            ':complete': data['complete_time'],
            ':status': 'COMPLETED'
        }
    )
    print("DynamoDB updated successfully.")


def publish_sns_message(topic_arn, message):
    sns = boto3.client('sns')
    try:
        response = sns.publish(
            TopicArn = topic_arn,
            Message = json.dumps({'default': json.dumps(message)}),
            Subject=f'Completion Notification: {topic_arn}',
            MessageStructure='json'
        )
        print(f"Notification sent successfully. Message ID: {response['MessageId']}")
    except Exception as e:
        print(f"Failed to send notification: {str(e)}")





class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

#./jobs/397717f3-d953-414c-88a2-6ef6cde203d0/397717f3-d953-414c-88a2-6ef6cde203d0~test.vcf


if __name__ == '__main__':
    # Load configuration
    config = ConfigParser()
    config.read('ann_config.ini')
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
    cnet_id = config.get('info', 'cnet_id')
    user_prefix = config.get('info', 'user_id')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table_name)
    

    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        input_file_path = sys.argv[1].strip()
        user_id = sys.argv[2]
        job_id = input_file_path.split('/')[-2]
        

        with Timer():
            results_file = input_file_path.replace('.vcf', '.annot.vcf')
            log_file = (input_file_path + '.count.log').strip()
            driver.run(input_file_path, 'vcf')

        unique_id = os.path.basename(job_id).split('~')[0]
        results_file_name1 = results_file.split('/')[-1]
        results_file_name = results_file_name1.split('~')[-1]
        log_file_name1 = log_file.split('/')[-1]
        log_file_name = log_file_name1.split('~')[-1]

        s3_key_results_file = f"{cnet_id}/{user_prefix}/{unique_id}/{results_file_name}"
        s3_key_log_file = f"{cnet_id}/{user_prefix}/{unique_id}/{log_file_name}"
        path_to_del_local = os.path.dirname(results_file)

        upload_file_to_s3(s3_results_bucket, s3_key_results_file, results_file)
        upload_file_to_s3(s3_results_bucket, s3_key_log_file, log_file)



        # Clean up local files
        delete_local_file(path_to_del_local)

        # Prepare data for DynamoDB update
        # { "N" : { "S" : "1716009696.830354" } }
        data = {
            's3_results_bucket': s3_results_bucket,
            's3_key_result_file': s3_key_results_file,
            's3_key_log_file': s3_key_log_file,
            'complete_time': int(time.time()),
        }
        update_dynamodb(job_id, data)

        # Publish notification to SNS result
        #message_result = {'message_type': 'result_message',
                          #'job_id': job_id,
                          #'s3_results_bucket': s3_results_bucket}
        
        #publish_sns_message(topic_arn_results, message_result)

        # Publish notification to SNS archive
        message_archive = {'message_type': 'archive_message',
                           'job_id': job_id,
                           'user_id': user_id}
        
        publish_sns_message(topic_arn_archive, message_archive)



    else:
        print("A valid .vcf file and job ID must be provided as input to this program.")
    

### EOF