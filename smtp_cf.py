import imaplib
import email
import pandas as pd
import io

def read_latest_excel_email(user, password, sender):
    imap_url = 'imap.gmail.com'
    my_mail = imaplib.IMAP4_SSL(imap_url)
    my_mail.login(user, password)
    my_mail.select('Inbox')
    key = 'FROM'
    value = sender
    _, data = my_mail.search(None, key, value)
    mail_id_list = data[0].split()
    latest_email_id = mail_id_list[-1]
    _, msg_data = my_mail.fetch(latest_email_id, '(RFC822)')
    msg = email.message_from_bytes(msg_data[0][1])
    for part in msg.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        filename = part.get_filename()
        if not filename:
            continue
        if '.xls' in filename or '.xlsx' in filename:
            file = io.BytesIO(part.get_payload(decode=True))
            df = pd.read_excel(file)
            return df

user = 'prakharrajgupta16011995@gmail.com'
password = ''
sender = 'dhiragupta1@gmail.com'
df = read_latest_excel_email(user, password, sender)
print(df.head())