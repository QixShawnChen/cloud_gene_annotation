# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
import datetime
import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from decimal import Decimal
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for, Flask, jsonify)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


# Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))
  job_id = s3_key.split('/')[-1].split('~')[0]

  # Extract the job ID from the S3 key

  # Persist job to database
  # Move your code here...
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table('qixshawnchen_annotations')
  job_info = {
    'job_id': job_id,
    'user_id': session['primary_identity'],
    'input_file_name': s3_key.split('/')[-1].split('~')[-1],
    "s3_inputs_bucket": bucket_name,
    "s3_key_input_file": s3_key,
    "submit_time": int(time.time()),
    "job_status": "PENDING"
  }
  table.put_item(Item=job_info)
  

  # Send message to request queue
  # Move your code here...
  sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
  # 'arn:aws:sns:us-east-1:659248683008:qixshawnchen_job_requests'
  topic_arn_requests = app.config['AWS_SNS_JOB_REQUEST_TOPIC']
  
  sns.publish(
    TopicArn=topic_arn_requests,
    Message=json.dumps({'default': json.dumps(job_info)}),
    MessageStructure='json'
  )
  print("message to annotator.py sent seuccessfully")

  return render_template("annotate_confirm.html", job_id = job_id)
  


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  user_id = session["primary_identity"]
  
  # Initialize DynamoDB resource
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table('qixshawnchen_annotations')
  try:
    # Query the table for annotations by the user
    response = table.query(
      IndexName='user_id-index',
      KeyConditionExpression=Key('user_id').eq(user_id)
    )
   
    items = response.get('Items', [])
    annotations = []
    for item in items:
      submit_time_stamp = int(item['submit_time'])
      dt_object1 = datetime.datetime.fromtimestamp(submit_time_stamp)
      offset = datetime.timedelta(hours=-5)
      dt_object = dt_object1 + offset
      formatted_datetime = dt_object.strftime('%Y-%m-%d %H:%M:%S')
      annotation = {
        'job_id': item['job_id'],
        'submit_time': formatted_datetime,
        'input_file_name': item['input_file_name'],
        'job_status': item['job_status']
      }
      annotations.append(annotation)

  except Exception as e:
    print(f"Error querying annotations: {str(e)}")
    annotations = []
  
  return render_template('annotations.html', annotations=annotations)


# Need a logic to manage the membership
"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  s3 = boto3.client('s3')
  user_id = session["primary_identity"]  # Get the currently authenticated user's ID
  if not user_id:
    abort(403)  # User is not authenticated

  # Initialize DynamoDB resource
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  
  # Initialize DynamoDB client
  dynamodb_client = boto3.client('dynamodb')
  
  try:  # Get DynamoDB Record
    response = dynamodb_client.get_item(
      TableName=app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'], 
      Key={'job_id': {'S': id}}
    )
  # Error - Table missing
  except dynamodb_client.exceptions.ResourceNotFoundException:
    abort(500)   

  # Get DynamoDB Record
  try:
    item = response['Item']
  # No result
  except KeyError:
    abort(404)
  
  if item['user_id']['S'] != session['primary_identity']: 
    abort(403)
  submit_time_stamp = int(item['submit_time']['N'])
  dt_object1 = datetime.datetime.fromtimestamp(submit_time_stamp)
  offset = datetime.timedelta(hours=-5)
  dt_object = dt_object1 + offset
  formatted_datetime = dt_object.strftime('%Y-%m-%d %H:%M:%S')
  annotation = {}
  annotation['job_id'] = item['job_id']['S']
  annotation['submit_time'] = formatted_datetime
  annotation['input_file_name'] = item['input_file_name']['S']
  annotation['job_status'] = item['job_status']['S']
  free_access_expired = False
  restore_message = False

  try:  # Download results file to user
    response_input = s3.generate_presigned_url(
      ClientMethod='get_object', 
      Params={
        'Bucket': app.config["AWS_S3_INPUTS_BUCKET"], 
        'Key': item['s3_key_input_file']['S']
      }, 
      ExpiresIn=120
    )
  except ClientError as e: 
    abort(500)
  annotation['input_file_url'] = response_input


  if annotation['job_status'] == 'COMPLETED' or annotation['job_status'] == 'RESTORED' or annotation['job_status'] == 'RESTORING' or annotation['job_status'] == 'ARCHIVED': 
    submit_time_stamp_complete = int(item['complete_time']['N'])
    dt_object1_complete = datetime.datetime.fromtimestamp(submit_time_stamp_complete)
    offset_complete = datetime.timedelta(hours=-5)
    dt_object_complete = dt_object1_complete + offset_complete
    formatted_datetime_complete = dt_object_complete.strftime('%Y-%m-%d %H:%M:%S')
    complete_time = float(item['complete_time']['N'])
    annotation['complete_time'] = formatted_datetime_complete

    if (time.time() - complete_time >= app.config['FREE_USER_DATA_RETENTION']) and session['role'] == 'free_user':             
      free_access_expired = True
    elif 's3_key_result_file' not in item.keys() and (time.time() - complete_time >= app.config['FREE_USER_DATA_RETENTION']):  # Files are being unarchived
      restore_message = True
      annotation['restore_message'] = "The file is currently being unarchived and should be available within several hours for Premium members. Please check back later. Note that if you cancel your membership, any files not yet archived will need to be archived again."
    else:  
      try:  # Download results file to user
        dynamodb_client = boto3.client('dynamodb')
        new_path1 = item['s3_key_log_file']['S']
        new_path = new_path1.replace('test.vcf.count.log', 'restored_test.annot.vcf')
        print(new_path)
        response = s3.generate_presigned_url(
          ClientMethod='get_object', 
          Params={
            'Bucket': app.config["AWS_S3_RESULTS_BUCKET"], 
            'Key': new_path
          }, 
          ExpiresIn=120
        )
        #annotation['result_file_url'] = response

      except ClientError as e: 
        abort(500)
      annotation['result_file_url'] = response
  #annotation['result_file_url'] = item['s3_key_log_file']['S']
  
  return render_template('annotation_details.html', annotation=annotation, free_access_expired=free_access_expired)



"""Display the log file contents for an annotation jobpa
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  # Get the currently authenticated user's ID
  user_id = session["primary_identity"]
  # Initialize DynamoDB client
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table('qixshawnchen_annotations')

  # Initialize S3 client
  s3_client = boto3.client('s3')
  try:
    # Retrieve annotation job details from DynamoDB
    response = table.get_item(Key={'job_id': id})
    annotation = response.get('Item', None)

    # Check if the job exists and belongs to the user
    if not annotation or annotation['user_id'] != user_id:
      abort(403, description="Not authorized to view this job")

    # Fetch log file from S3
    if 's3_key_log_file' in annotation:
      log_file_key = annotation['s3_key_log_file']
      bucket_name = annotation['s3_results_bucket']
      log_obj = s3_client.get_object(Bucket=bucket_name, Key=log_file_key)
      log_file_contents = log_obj['Body'].read().decode('utf-8')

      return render_template('view_log.html', job_id=id, log_file_contents=log_file_contents)

  except Exception as e:
    print(f"Error retrieving or displaying log file: {str(e)}")
    abort(500, description="Internal Server Error")  # Handle server errors gracefully

  return abort(404, description="Log file not found")




"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!
    user_id = session['primary_identity']
    glacier_vault = app.config['AWS_GLACIER_VAULT']
    topic_arn_restore = app.config['AWS_SNS_JOB_RESTORE_TOPIC']
    sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])

    
    message_restore = {'message_type': 'restore_message',
                          'user_id': user_id}

    try:
      response = sns.publish(
        TopicArn = topic_arn_restore,
        Message = json.dumps({'default': json.dumps(message_restore)}),
        Subject=f'Completion Notification: {topic_arn_restore}',
        MessageStructure='json'
      )
      print(f"Notification to restore.py sent successfully. Message ID: {response['MessageId']}")
    except Exception as e:
      print(f"Failed to send notification: {str(e)}")

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF