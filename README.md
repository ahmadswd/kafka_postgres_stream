# Kafka producer consumer exampel using kafka-python and PostgresSQL database


This example shows how to configure kafak instance using cloud based instance with producer that sends random login data to the instance and a provider that receives the login data and store it in realtime to postgresql database.

## Setting up kafka cluster: 
- free version available thanks to upstash [Upstash](https://console.upstash.com/) signup using the free version
- choose the following options:
    - Kafka cluster
    - create new cluster
    - keep the settings as is for testing purposes
    - once the cluster is created create a new topic: topic represents the specific topic you want to use for specific information between producer and consumer.

## Producer configuration:
- use [producer.py](./producer.py) file added tp the following folder.
- change the following information to connect to your cluster:
![images/config.png](./images/config.png)
    - ```bootstrap_servers=['*********']``` for this one put the cluste Endpoint value from above image: ***use your own cluster Endpoint***
    - ```sasl_plain_username='*********'``` for this one put the cluster Username value from above image: ***use your own cluster Username***
    - ```sasl_plain_password='*********'``` for this one put the cluster Password value from above image: ***use your own cluster Password***
    - ```producer.send('*********', data)``` for this one put the topic name you created - to check the topics you created, refernce to the Topics tab from above image and it should look like this: ***signup_topic***  ***use your the topic name you created if it's different than the one below*** 
    ![images/topics.png](./images/topics.png)

    > **_NOTE:_**  for producer we use   ```value_serializer = lambda x: json.dumps(x).encode('utf-8')``` to make sure the data is being serialized as JSON


## Consumer configuration:
- use [consumer.py](./consumer.py) file added tp the following folder.
- change the following information to connect to your cluster:
![images/config.png](./images/config.png)
    - line number 11:  ```'*********'``` is for the topic name created in the producer configuration example below have ***signup_topic*** as the name
    ![images/topics.png](./images/topics.png)
    - ```bootstrap_servers=['*********']``` for this one put the cluste Endpoint value from above image: ***use your own cluster Endpoint***
    - ```sasl_plain_username='*********'``` for this one put the cluster Username value from above image: ***use your own cluster Username***
    - ```sasl_plain_password='*********'``` for this one put the cluster Password value from above image: ***use your own cluster Password***.

    > **_NOTE:_**  for producer we use   ```value_deserializer=lambda x: loads(x.decode('utf-8'))``` to make sure the data is being deserialized as JSON from the producer

## Postgres SQL Server configuration
- either use local or remote postgresql server.
- after createing the server get the following infomation:
    - hostname
    - username
    - password
    - database
- use the infomation from above to fill the following script in the consumer to allow it access to the database instance
```engine = create_engine("postgresql+psycopg2://hostname:username@password:5432/database")```.
- Create table names ***signup*** "based on our example but feel free to call it whatever you want" and run the following script to create it.


> **_NOTE:_**  make sure the postgresql server is running.

> **_NOTE:_**  postgresql server is one option but data could be stored into MongoDB, or just regular json files, it depends on the usecase.

## Testing the full configuration:
- Open terminal or gitbash and use the following command to start the ***producer***
    - ```python producer.py```
    
    > **_NOTE:_**  Keep the producer running.
- Open another terminal or gitbash and use the following command to start the ***consumer***
    - ```python consumer.py```

## Validation:
- after both services are running, the consumer will start to consume the data incoming from the producer in real time, parse it from json and insert it into the postgresql database.
- go to kafka server and check the Usage tab:
![images/usage.png](./images/usage.png)
- also validate on postgresql server run the following sql query to check the table being populated with the new data as it's expected
```SELECT * FROM signup```
![images/postgressql.png](./images/postgressql.png)
    
## References:
for additional information and references see the following useful links:
- https://content.altexsoft.com/media/2020/03/etl_pipeline.png
- https://www.tutorialspoint.com/apache_kafka/apache_kafka_cluster_architecture.htm
- https://www.openlogic.com/blog/kafka-partitions
