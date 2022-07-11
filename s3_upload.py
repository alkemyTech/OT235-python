import boto3
from botocore.exceptions import NoCredentialsError

ACCESS_KEY = 'AKIA24X5Z5Y2H4TL52MP'
SECRET_KEY = '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'


def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


uploaded = upload_to_aws('lat_sociales_data.txt', 'alkemy-acceleracion-lugonesnicolas', 'lat_sociales_data.txt')
uploaded = upload_to_aws('kennedy_data.txt', 'alkemy-acceleracion-lugonesnicolas', 'kennedy_data.txt')