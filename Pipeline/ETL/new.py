import smtplib, ssl


port = 465  # For SSL
smtp_server = "smtp.gmail.com"
sender_email = "jaygames740555@gmail.com"  # Enter your address
receiver_email = "jaykapadia22@gmail.com"  # Enter receiver address
password = "xzxeanjkviuahtpl"
message = """\
Subject: Hi there


This message is sent from."""

context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message)

# B:\Python\venv\Scripts\python.exe
# B:\Python\Pipeline
