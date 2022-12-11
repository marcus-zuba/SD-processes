import boto3

def download_file_to_process(filename):
    s3 = boto3.client('s3')
    with open(filename, 'wb') as f:
        s3.download_fileobj('sd-mvp-files-to-process', filename, f)

def upload_processed_file(filename):
    s3 = boto3.client('s3')
    with open(filename, 'r') as f:
        s3.upload_file(filename, 'sd-mvp-files-processed', filename)