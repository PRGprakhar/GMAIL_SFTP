import paramiko
import os
from google.cloud import storage, bigquery
import datetime

# SFTP Configuration
host = ''
port = 22
username = ''
password = ''
sftp_dir_path = ''
bucket_name = 'sftp_voucher_code'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/seuk_digitas/KEYS/samsunguk-media-support-5ea7ffbeecf3.json'
PROJECT_ID = 'samsunguk-media-support'
dataset = 'DTC_Commercial'
table_name = 'Voucher_code'
DATE = datetime.date.today().strftime('%Y%m%d')
transport = paramiko.Transport((host, port))
transport.connect(username=username, password=password)
sftp = paramiko.SFTPClient.from_transport(transport)
files = sftp.listdir(sftp_dir_path)
bigquery_client = bigquery.Client(project=PROJECT_ID)
schema = [
    bigquery.SchemaField('Site', 'STRING'),
    bigquery.SchemaField('Sales_Application', 'STRING'),
    bigquery.SchemaField('Order_ID', 'STRING'),
    bigquery.SchemaField('Line_Item_ID', 'STRING'),
    bigquery.SchemaField('SKU', 'STRING'),
    bigquery.SchemaField('Order_Entry_Total_Price', 'FLOAT'),
    bigquery.SchemaField('Line_Item_Sale_Price', 'FLOAT'),
    bigquery.SchemaField('Order_Entry_Quantity', 'INTEGER'),
    bigquery.SchemaField('Voucher_Code', 'STRING'),
    bigquery.SchemaField('Voucher_Amount', 'FLOAT'),
    bigquery.SchemaField('Payment_Mode', 'STRING'),
]
job_config = bigquery.LoadJobConfig()
job_config.schema = schema
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
job_config.skip_leading_rows = 1
for filename in files:
    if DATE in filename:
        # Upload to Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob_name = filename
        blob = bucket.blob(blob_name)
        file_path =sftp_dir_path + '/' + filename
        with sftp.file(file_path,"rb") as f:
            blob.upload_from_file(f)
        # Load data from GCS to BigQuery
        source_uri='gs://{}/{}'.format(bucket_name, blob_name)
        dataset_ref = bigquery_client.dataset(dataset)
        table_ref = dataset_ref.table(table_name)        
        load_job = bigquery_client.load_table_from_uri(
            source_uris=source_uri,
            destination=table_ref,
            job_config=job_config,
        )
        load_job.result()  # Wait for the job to complete

        if load_job.errors:
            for error in load_job.errors:
                print("Error")
        else:
            print("Data uploaded")

# Close SFTP connection
sftp.close()
transport.close()

#This has been used as a final code,just functionising it is left
