
import boto3

s3 = boto3.resource(
    's3',
    aws_access_key_id='your_id',
    aws_secret_access_key='your_key'
)
bucket = s3.Bucket('your_bucket_name')

print('uploading process')
data = open('your_filename', 'rb')
bucket.put_object(Key='your_data_path', Body=data)


