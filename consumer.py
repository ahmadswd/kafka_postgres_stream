from kafka import KafkaConsumer
from json import loads
import time

from sqlalchemy import create_engine
from sqlalchemy import text

engine = create_engine("postgresql+psycopg2://*********:*********@*********:5432/*********")

consumer = KafkaConsumer(
  '*********',
  bootstrap_servers=['*********',],
  sasl_mechanism='SCRAM-SHA-256',
  security_protocol='SASL_SSL',
  sasl_plain_username='*********',
  sasl_plain_password='*********',
  group_id='$GROUP_NAME',
  auto_offset_reset='earliest',
  value_deserializer=lambda x: loads(x.decode('utf-8'))

)

time.sleep(2)

for msg in consumer:
    print(msg)
    timestamp = msg.timestamp
    value = msg.value
    with engine.connect() as conn:
        conn.execute(text(
            f"INSERT INTO signup VALUES('{value['username']}','{value['password']}','{value['firstname']}','{value['lastname']}','{value['email']}',{timestamp}); commit;"
        ))
        print("Data inserted successfully!")

    


