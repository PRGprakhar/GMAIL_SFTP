import os
import io
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date, timedelta
from google.oauth2.credentials import Credentials
from oauth2client.client import AccessTokenCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import requests
import httplib2
import urllib.request
from datetime import datetime
from urllib.parse import urlencode
from google.cloud import bigquery
from google.cloud import storage
from bs4 import BeautifulSoup
from google.api_core.exceptions import NotFound
from email.mime.application import MIMEApplication
from googleapiclient.http import MediaFileUpload
from email.mime.base import MIMEBase
from email import encoders
from io import StringIO
from pandas_gbq import to_gbq

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly','https://www.googleapis.com/auth/gmail.send']

CREDS = "/home/seuk_digitas/Scripts/gmail_service/samsunguk-media-support-dcb883668e19.json"
bigquery_client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS)  

def access_token_from_refresh_token(client_id, client_secret, refresh_token):
    response = requests.post('https://accounts.google.com/o/oauth2/token',
    data=urlencode({
      'grant_type':    'refresh_token',
      'client_id':     client_id,
      'client_secret': client_secret,
      'refresh_token': refresh_token
    }),
    headers={
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/json'
    }
    )
#     print(response.json())
    return response.json()['access_token']


# service to create gmail token
def create_gmail_service():
    creds = None

    # The file where you will store the credentials after authorization.
    token_file = "/home/seuk_digitas/Scripts/gmail_service/gmail_token.json"

    # Check if the token file already exists.
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    # ==========================================
    CLIENT_ID = creds.client_id
    CLIENT_SECRET = creds.client_secret
    REFRESH_TOKEN= creds.refresh_token

    access_token = access_token_from_refresh_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
    print("------------------------------------------Access Token Refereshed-----------------------------------------------")
    credentials = AccessTokenCredentials(access_token, "MyAgent/1.0", None)
    http = credentials.authorize(httplib2.Http())
    print('API Request Authenticated Successfully!!')
    # Build the Gmail service.
    service = build('gmail', 'v1', credentials=creds)
    return service

# to fetch excel attachement from email
def get_excel_attachment_from_email(service,msg):
    # Check if the message contains attachments.
    if 'parts' in msg['payload']:
        for part in msg['payload']['parts']:
            if 'filename' in part:
                filename = part['filename']
                # Check if the attachment is an Excel file.
                if filename.endswith('.xlsx') or filename.endswith('.csv'):
                    attachment_data = part['body']['attachmentId']
                    attachment = service.users().messages().attachments().get(userId='me', messageId=msg['id'], id=attachment_data).execute()
                    file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                    return file_data, filename
    return None, None

# listing emails and fetching the email based on subject filter and get altest email.
def fetch_emails(service, subject_query=''):
    # Fetch emails from the Gmail account using the specified subject query (optional).
    query = "subject:"+subject_query
    messages = service.users().messages().list(userId='me', q=query).execute()
    if 'messages' in messages:
        latest_email = None
        for message in messages['messages']:
            msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
            # Check if the message has threads.
            if 'threadId' in msg:
                # Get the threads associated with the message.
                threads = service.users().threads().list(userId='me', q="subject:"+subject_query).execute()
                if 'threads' in threads:
                    # Check if there is at least one more thread.
                    if len(threads['threads']) >= 1:
                        # Get the second thread (index 1) in the list.
                        thread_id = threads['threads'][0]['id']
                        # Get the messages within the second thread.
                        thread_messages = service.users().threads().get(userId='me', id=thread_id).execute()
                        # Process each message in the thread.
                        for message in thread_messages['messages']:
                            msg = service.users().messages().get(userId='me', id=message['id']).execute()
                            # Check if the message contains attachments and get the Excel attachment.
                            excel_attachment, filename = get_excel_attachment_from_email(service,msg)
                            if excel_attachment:
                                # This is the most recent email with the matching subject containing an Excel attachment.
                                latest_email = (excel_attachment, filename)
        if latest_email:
            return latest_email


# upload the source report into Big query
# upload the source report into Big query
def upload_dataframe_to_bigquery(dataframe, project_id, dataset_id, table_id):
     #Authenticate with GCP
     client = bigquery.Client(project=project_id)

     # Define the dataset and table references
     dataset_ref = client.dataset(dataset_id)
     table_ref = dataset_ref.table(table_id)

     schema = [
     {'name': 'Order_ID', 'type': 'STRING'}
              ]
                                                
     # Upload the DataFrame to BigQuery
     to_gbq(dataframe, destination_table='DTC_Commercial.CSS_Sales', project_id='samsunguk-media-support', if_exists='replace', table_schema=schema)
     print("Data uploaded successfully to BigQuery.")
                                                        




# create email with attachement   
''' 
def create_message_with_attachment(sender, to, subject, message_text, file_path):
    message = MIMEMultipart()
    message['to'] = to
    message['subject'] = subject

    msg = MIMEBase('application', 'octet-stream')
    msg.set_payload(open(file_path, 'rb').read())
    encoders.encode_base64(msg)
    msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
    message.attach(msg)

    message.attach(MIMEBase('application', 'plain'))
    message.attach(MIMEBase('application', 'octet-stream'))

    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
'''

def create_message_with_attachment(sender, to, subject, message_text, dataframe):
    message = MIMEMultipart()
    message['to'] = to
    message['subject'] = subject

    # Convert the DataFrame to a CSV string
    csv_data = dataframe.to_csv(index=False)

    msg = MIMEText(message_text, 'html')
    message.attach(msg)

    # Attach the CSV data as a MIMEText part
    attachment = MIMEText(csv_data)
    attachment.add_header('Content-Disposition', 'attachment', filename='CSS_Sales.csv')
    message.attach(attachment)

    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

# send email
def send_message(service, user_id, message):
    try:
        print("message no")
        message = service.users().messages().send(userId=user_id, body=message).execute()
        print("Message Id:"+message['id'])
        return message
    except HttpError as error:
        print("An error occurred:"+ error)


if __name__ == '__main__':
    
    # create gmail service
    service = create_gmail_service()
    # Replace 'Your_Subject_Query' with the subject you want to filter emails on.
    attachment_info = fetch_emails(service, subject_query='SEUK CSS Sales')
    
    if attachment_info:
        excel_attachment, filename = attachment_info
        df_input=pd.read_csv(StringIO(str(excel_attachment,'utf-8')),dtype={'po_id': str})
        df_input.rename(columns = {'po_id':'Order_ID'}, inplace = True)
        print(df_input.head())   
    else:
        print("No email with the specified subject or Excel attachment found.")


    PROJECT_ID='samsunguk-media-support'
    DATASET_ID = 'DTC_Commercial'
    TABLE_NAME = 'CSS_Sales'
    #upload data frame in BQ
    upload_dataframe_to_bigquery(df_input, PROJECT_ID, DATASET_ID, TABLE_NAME)

    # query to fetch sales data for the attached order id's
    query_job = bigquery_client.query(""" 
    Create or Replace View   samsunguk-media-support.DTC_Commercial.vw_CSS_Sales as
    select    line_id, country, USER_GROUP, TRANSACTION_TYPE, DATE, CHANNEL, a.Order_ID, sku, Order_value_ex_vat, cancelled_ex_vat, returned_ex_vat, suspended_ex_vat, Revenue_ex_vat, order_value_usa, cancelled_usa, returned_usa, suspended_usa, revenue_ex_vat_usa, order_quantity, cancelled_quantity, returned_quantity, suspended_quantity, quantity, voucher_code, UpgradeLounge, Payment_Mode, Added_Services, TradeIn_SUP, Insurance, Delivery_Mode, Program_Type, Exchange_Type, Exchange_Brand, Exchange_Category, Exchange_Device_Name, Customer_Login_Type, Tradein_IMEI, Exchange_Value, External_Service_Type, Rewards_Customer_Type, Rewards_Amount, Estimated_Accrued_Amount, Spend_Amount, Offer_id, Promo_type, Promotion, Price_Drop, Device_Type from 
    (select distinct Order_ID from samsunguk-media-support.DTC_Commercial.CSS_Sales where Order_ID is not null or Order_ID <>'"') a 
    left join samsunguk-media-support.DTC_Commercial.FMT_eStoreSales_Exclude_Returns b
    on a.Order_ID= b.Order_ID ;""")
 
    # Save the results in a Data Frame
    results = query_job.result() # Waits for job to complete.
    job=bigquery_client.query (' SELECT * FROM `samsunguk-media-support.DTC_Commercial.vw_CSS_Sales` ')
    df_out =job.result().to_dataframe()
    print(df_out.head())
    #file_path = 'CSS_Sales.csv'
    #df_out.to_csv(file_path, index=False)

    
    # Email details
    sender_email = 'seuk.digitas@gmail.com'
    receiver_email = 'basangi.patnaik@publicisgroupe.com;bethany.chickah@digitas.com;seyi.kareem@digitas.com;prakhar.gupta1@publicisgroupe.com'
    subject = 'Automated CSS Sales Report'
    #body = 'Please find the attached DataFrame.'
    body ="""<p>Hi All,<br>
                        <br>
                        Attached the updated css report.<br>
                        <br>
                        <br>
                        Thanks,<br>
                        Basangi Gowtham Patnaik</p>
    """

    # Create the email message with attachment
    message = create_message_with_attachment(sender_email, receiver_email, subject, body, df_out)
    #print(message)
    # Send the email
    send_message(service, 'me', message)
    print(" CSS Automated report sent successfully !")
