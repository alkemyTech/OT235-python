from airflow.hooks.S3_hook import S3Hook
import os

"""
DAG para guardar en un bucket S3 datos procesados 
de las universidades del grupo F.
"""

s3_conn = "f_universities"

#funcion para obtener path de los files a subir
def get_files_to_upload(dir):

    files = []

    for file in os.listdir(dir):
        file_path = os.path.join(dir, file)
        files.append(file_path)

    return files

#función para subir los files a s3
def upload_to_s3(path: str, key: str, bucket_name: str) -> None: 
    hook = S3Hook(s3_conn)
    hook.load_file(
        filename=path,
        key=key,
        bucket_name=bucket_name
)

#obtiene los path de los files a subir
files = get_files_to_upload("/files")

#loop sobre los files obtenidos para subirlos a s3
for file in files:

    filename = file.split("/")

    path = file
    key = filename
    bucket_name = "bucket"

    upload_to_s3(path, key, bucket_name)

