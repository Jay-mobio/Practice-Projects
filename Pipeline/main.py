
import smtplib, ssl


port = 465  # For SSL
smtp_server = "smtp.gmail.com"
sender_email = "jaygames740555@gmail.com"  # Enter your address
receiver_email = "jaykapadia22@gmail.com"  # Enter receiver address
password = "xzxeanjkviuahtpl"
message = """\
Subject: Hi there
."""

context = ssl.create_default_context()
from transformation import Transformation
import json
import os
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message)
class Engine:
    def __init__(self,dataSource,dataSet):
        trans_obj = Transformation(dataSource, dataSet)

if __name__ == '__main__':
    elt_data = json.load(open(os.getcwd()+'\config.json'))


    for dataSource, dataSet in elt_data['data_sources'].items():
        print(dataSource)
        for data in dataSet:
            print(data)
            main_obj = Engine(dataSource, data)

