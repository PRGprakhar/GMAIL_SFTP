import paramiko
import os
from google.cloud import storage

host_name = ''
host=''
port = 22
username = ''
password = ''
sftp_file_path='/root_storage/external/seuk-local-ftp/SEUK_hybris_vouchers/hybris_uk_voucher_report20231011084139.csv'
blob_name='hybris_uk_voucher_report20231011084139.csv'
bucket_name='sftp_voucher_code'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/seuk_digitas/KEYS/samsunguk-media-support-5ea7ffbeecf3.json'
def upload_file_to_gcs(sftp_file_path, bucket_name, blob_name):
    transport = paramiko.Transport((host, port))
    transport.connect(None, username, password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with sftp.open(sftp_file_path, "rb") as f:
        blob.upload_from_file(f)
upload_file_to_gcs(sftp_file_path, bucket_name, blob_name)
