from kafka import KafkaProducer
import time
import json
import random
producer = KafkaProducer(
  bootstrap_servers=['*********'],
  value_serializer = lambda x: json.dumps(x).encode('utf-8'),
  sasl_mechanism='SCRAM-SHA-256',
  security_protocol='SASL_SSL',
  sasl_plain_username='*********',
  sasl_plain_password='*********',
)


# publish random message to Kafka every 5 seconds
uname = ["Ahmad","Sam","Jack","John","Kelly"]
password = ["123543","abcsdq13","fg5fd45","dsdfhtry3","dhtrytry55"]
firstname = ["Ahmad","Sam","Jack","John","Kelly"]
lastname = ["Rolland","Smith","Hilton","Sweed"]
email = ["asdas@g.com","aswqedas@g.com","wer453@g.com","ghfg45@g.com",]
for i in range(1000):
    data = {"username":random.choice(uname)
            ,"password":random.choice(password)
            ,"firstname":random.choice(firstname)
            ,"lastname":random.choice(lastname)
            ,"email":random.choice(email)}
    print("Sending data right now...")
    producer.send('*********', data)
    time.sleep(5)
