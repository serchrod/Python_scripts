import boto3
import botocore

s3session = boto3.session.Session(
    aws_access_key_id='your_id',
    aws_secret_access_key='your_key'
)

BUCKET_NAME = 'your_bucket_name' # replace with your bucket name
KEY = 'your_path' # replace with your object key

s3 = s3session.resource('s3')

try:
    s3.Bucket(BUCKET_NAME).download_file(KEY, 'local_filename')
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise


