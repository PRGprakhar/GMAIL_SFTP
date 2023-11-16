import os
from google.cloud import bigquery


def facebook_zeus_csv_loader(data, context):
    client = bigquery.Client()
    dataset_id = os.environ['DATASET']
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
            bigquery.SchemaField('Date', 'STRING' ),
            bigquery.SchemaField('Adname', 'STRING' ),
            bigquery.SchemaField('Placement', 'STRING' ),
            bigquery.SchemaField('Campaign', 'STRING' ),
            bigquery.SchemaField('Platform', 'STRING' ),
            bigquery.SchemaField('Account', 'STRING' ),
            bigquery.SchemaField('Impressions', 'NUMERIC' ),
            bigquery.SchemaField('Currency', 'STRING' ),
            bigquery.SchemaField('spendGBP', 'NUMERIC' ),
            bigquery.SchemaField('Video25', 'NUMERIC' ),
            bigquery.SchemaField('Video50', 'NUMERIC' ),
            bigquery.SchemaField('Video75', 'NUMERIC' ),
            bigquery.SchemaField('VideoViews', 'NUMERIC' ),
            bigquery.SchemaField('VideoCompletions', 'NUMERIC' ),
            bigquery.SchemaField('Comments', 'NUMERIC' ),
            bigquery.SchemaField('Shares', 'NUMERIC' ),
            bigquery.SchemaField('PageLikes', 'NUMERIC' ),
            bigquery.SchemaField('Clicks', 'NUMERIC' ),
            bigquery.SchemaField('PostEngagements', 'NUMERIC' ),
            bigquery.SchemaField('Landing_Page_Views', 'NUMERIC' ),
            bigquery.SchemaField('ATC_FB', 'NUMERIC' ),
            bigquery.SchemaField('Orders_FB', 'NUMERIC' ),
            bigquery.SchemaField('Revenue_FB', 'NUMERIC' ),
            bigquery.SchemaField('Reporting_Starts_Ignore', 'STRING' ),
            bigquery.SchemaField('Reporting_Ends_Ignore', 'STRING' ),
            bigquery.SchemaField('Ad_set_name', 'STRING' )
    ]
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    # get the URI for uploaded CSV in GCS from 'data'
    uri = 'gs://' + os.environ['BUCKET'] + '/' + data['name']

    # lets do this
    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(os.environ['TABLE']),
        job_config=job_config)

    print('Starting job {}'.format(load_job.job_id))
    print('Function=facebook_zeus_csv_loader, Version=' + os.environ['VERSION'])
    print('File: {}'.format(data['name']))

    load_job.result()  # wait for table load to complete.
    print('Job finished.')

    destination_table = client.get_table(dataset_ref.table(os.environ['TABLE']))
    print('Loaded {} rows.'.format(destination_table.num_rows))
