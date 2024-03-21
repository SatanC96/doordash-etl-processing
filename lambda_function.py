import json
import boto3
import pandas as pd
from io import StringIO

def lambda_handler(event, context):
    # Get the S3 bucket and object key from the Lambda event trigger
    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Use boto3 to get the json file from S3
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_content = response["Body"].read().decode('utf-8')
    json_content = json.loads(file_content)

    # Read the content using pandas
    data = pd.read_json(StringIO(file_content))
    print(data)
    new_data = data[data["status"]=="delivered"]
    print(new_data)
    new_json = new_data.to_json(orient = 'records')
    print(new_json)
    
    fileName = 'modified_raw_input' + '.json'
    s3 = boto3.resource('s3')
    target_bucket = s3.Bucket('doordash-target-zn-123')
    target_bucket.put_object(Key=fileName, Body=new_json)