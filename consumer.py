import asyncio
from kafka import KafkaConsumer
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

consumer = KafkaConsumer(
    'jobs',
    bootstrap_servers=['localhost:9092'],
    group_id='OYdlvsrvTzaaCbLVniVC1Q'  
)

sender_email = "YOUR_EMAIL@gmail.com"
sender_password = "YOUR_PASSWORD"
receiver_email = "THEIR_EMAIL@gmail.com"

def send_email(subject, body):
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    message.attach(MIMEText(body, "plain"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(message)

async def consume_data():
    print("consumer data called",consumer)

    for message in consumer:
        data_item = message.value.decode('utf-8')
        subject = "Processed Data"
        send_email(subject, data_item)

loop = asyncio.get_event_loop()
loop.run_until_complete(consume_data())
