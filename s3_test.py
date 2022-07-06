import boto3
import logging

def upload_to_s3():
    s3_client = boto3.client('s3')
    bucket='name'
    file_name1='lat_sociales.txt'
    file_name2='kennedy.txt'

    response = s3_client.upload_file(file_name1,bucket,file_name1)
    response = s3_client.upload_file(file_name2,bucket,file_name2)

upload_to_s3()