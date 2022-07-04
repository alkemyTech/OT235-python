import os
from pathlib import Path
from airflow.hooks.S3_hook import S3Hook


def upload_to_s3(bucket_name: str) -> None:
    '''Upload files to a S3 bucket.
    '''
    CONN_ID = 's3_conn_univ_E'
    # get txt files with processed data from 'files' folder
    # files_txt is a list of dictionaries
    files_txt = get_txt_file_paths()

    hook = S3Hook(CONN_ID)
    for file in files_txt:
        hook.load_file(filename=file['file_path'],
                       key=file['file_name'],
                       bucket_name=bucket_name)


def get_txt_file_paths():
    '''Search for txt files in 'files' folder and
       returns a list of dictioraries with file names and
       file paths.
    '''
    DIR = Path(__file__).resolve().parent.parent
    FILES_DIR = str(DIR) + '/files'
    paths_txt = []
    for file in os.listdir(FILES_DIR):
        if file.endswith(".txt"):
            file_path = os.path.join(FILES_DIR, file)
            paths_txt.append({'file_name': file, 'file_path': file_path})

    return paths_txt
