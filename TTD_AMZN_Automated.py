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

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly','https://www.googleapis.com/auth/gmail.send']

CREDS = "/home/seuk_digitas/Scripts/gmail_client_secrets.json"
gcs_client = storage.Client.from_service_account_json(json_credentials_path=CREDS) 
bq_client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS) 
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/baspatna/Desktop/Samsung Digitas/documents/Cnfidential/samsunguk-media-support-5ea7ffbeecf3.json'
yesterday = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
ttd_date = (date.today()- timedelta(days = 30)).strftime('%Y-%m-%d')

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
    token_file = "/home/seuk_digitas/Scripts/gmail_token.json"

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

def amazon_process(file):
#     pd.to_datetime(file["Date"]).dt.strftime('%Y-%m-%d')
    file = file[['Date','Creative','Total cost']]
    file['Date'] = file['Date'].astype('str')
    file.columns = ['Date','Creative','Spend']
    return file


def ttd_process(file,date):
#     pd.to_datetime(file["Date"]).dt.strftime('%Y-%m-%d')
    file = file[['Date','Creative','Advertiser Cost (Adv Currency)']]
    file.columns = ['Date','Creative','Spend']
    file['Spend'] = file['Spend'].astype('int64')
    file = file.loc[file['Date']>=date]
    file['Date'] = file['Date'].astype('str')
    return file

def find_link_filename(mail):
    links_found = []
    for text in mail.split():
        if text.startswith('creative_spends'):
            filename = text.split('<')[0]
            text = text.split('<')[1]
            link = text.split('>')[0]
            links_found.append(link)
        elif text.startswith('Download'):
            text = text.split('<')[1]
            link = text.split('>')[0]
            links_found.append(link)
            filename = link.split('%2F')[-1].split('%')[0]
    if len(links_found)>0:
        return links_found[0],filename
    else:
        return 'None'

def find_link_ttd(mail):
    for text in mail.split():
        if text.startswith('https'):
            return text
        else:
            pass

# listing emails and fetching the email based on subject filter and get altest email.
def fetch_emails(service,platform, subject_query=''):
    # Fetch emails from the Gmail account using the specified subject query (optional).
    query = 'subject:{}'.format(subject_query)
    results = service.users().messages().list(userId='me', labelIds=['INBOX'], q=query).execute()
    messages = results.get('messages',[]);
    if not messages:
        print('No new messages.')
    else:
        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id']).execute()
            if platform=='amazon':
                for part in msg['payload']['parts']:
                    if part['partId']=='0':
                            for nested_part in part['parts']:
                                data = nested_part['body']["data"]
                                byte_code = base64.urlsafe_b64decode(data)
                                text = byte_code.decode("utf-8")
                                text_msg = str(text)
                                return text_msg
            elif platform=='ttd':
                data =msg['payload']['body']['data']
                byte_code = base64.urlsafe_b64decode(data)
                text = byte_code.decode("utf-8")
                text_msg = str(text)
                return text_msg
            
def upload_to_gcs(client,bucket_name,filename):
    bucket = client.get_bucket(bucket_name)
    object_name_in_gcs_bucket = bucket.blob(filename)
    object_name_in_gcs_bucket.upload_from_filename(filename)
    return 'Uploaded {} to bucket:{}'.format(filename,bucket_name)


def bq_export(bq_table,dataframe):
#     dataframe.columns = [''] * len(dataframe.columns)
    dataframe.to_gbq('Analytics_UK_Samsung.{}'.format(bq_table), 
                     'samsunguk-media-support',
                     chunksize=None,
                     if_exists='append'
                     )
    return 'Success'


def send_failure_mail(service, user_id,date,error):
    message = MIMEMultipart()
    message['to'] = 'prakhar.gupta1@publicisgroupe.com'
    message['from'] = 'seuk.digitas@gmail.com'
    message['subject'] = 'AMAZON/TTD Script Failure Alert!'
    message_text ="""
Hi All,

        Automated data upload/transfer for Amazon/TTD has failed for {}. Here is the error: 
        {}
        
Thanks,
Varsha
    """.format(date,error)
    msg = MIMEText(message_text)
    message.attach(msg)
    body = {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}
    try:
        message = service.users().messages().send(userId=user_id, body=body).execute()
        return message
    except HttpError as e:
        print("An error occurred: {}".format(e))

        
if __name__ == '__main__':
    try:
        # create gmail service
        service = create_gmail_service()
        #-----------------------------------Amazon----------------------------------------------------#
        amzn_mail = fetch_emails(service,'amazon', subject_query='creative spends')
        amzn_link,file = find_link_filename(amzn_mail)
        if yesterday in file:
            urllib.request.urlretrieve(amzn_link, file)
            amzn_report = pd.read_excel(file)
            processed_amzn_report = amazon_process(amzn_report)
            bq_export('zeus_amazon_paid',processed_amzn_report)
#             processed_amzn_report.to_csv('amazondata.csv',index=False)
#             upload_to_gcs(gcs_client,'zeus-amazon-paid','amazondata.csv')
            print('Amazon data for {} is successfully uploaded to BQ TABLE'.format(yesterday))
            os.remove(file)
        else:
            print('No Amazon mail found for {}'.format(yesterday))
        #--------------------------------------TTD----------------------------------------------------#
        ttd_mail = fetch_emails(service,'ttd', subject_query='Report Available: Daily Recurring Spend - Vivaki_AOD_UK_Starcom_Samsung')
        ttd_link = find_link_ttd(ttd_mail)
        ttd_file_name = "Vivaki_AOD_UK_Starcom_Samsung _ Last30Days _ Daily Recurring Spend_{}_{}.xlsx".format(ttd_date,ttd_link.split('/')[-1].split('?')[0])
        report_date_ttd = datetime.strptime(ttd_mail.split('Range')[1].split()[3], '%Y/%m/%d').strftime('%Y-%m-%d')
        if report_date_ttd==yesterday:
            urllib.request.urlretrieve(ttd_link, ttd_file_name)
            ttd_report = pd.read_excel(ttd_file_name)
            ttd_processed_report = ttd_process(ttd_report,'2023-11-07')
            bq_export('zeus_tradedesk_paid',ttd_processed_report)
#             ttd_processed_report.to_csv('ttd.csv',index=False)
#             upload_to_gcs(gcs_client,'zeus-tradedesk-paid','ttd.csv')
            print('TTD data for {} is successfully uploaded to BQ TABLE'.format(yesterday))  
            os.remove(ttd_file_name)
        else:
            print('No TTD mail found for {}'.format(yesterday))
    except Exception as e: 
        print(e)
        send_failure_mail(service, 'me',yesterday,e)
