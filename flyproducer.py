import time
import kafka
import pandas as pd
from kafka import KafkaProducer
from time import sleep
import json
import requests
from datetime import datetime
import mysql.connector as mc

api_key = 'cf12ce3fd6fd9263a13999c7144b13be'
topicfly = 'lv1'
brokers = ['cnt7-naya-cdh63:9092']
producer = KafkaProducer(bootstrap_servers=brokers)

host = 'localhost'
mysql_port = 3306
mysql_database_name = 'classicmodels'
mysql_table_name = 'airport_dep_iata'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'

mysql_conn = mc.connect(
    user=mysql_username,
    password=mysql_password,
    host=host,
    port=mysql_port,
    autocommit=True,
    database=mysql_database_name
)

df = pd.read_sql('SELECT dep_iata FROM airport_dep_iata where status=1', con=mysql_conn)
mysql_conn.close()

while True:
    try:
        for index, row in df.iterrows():
            values = row.values # Get the values in the row as a list
            
            iata_value=values = ', '.join(str(value) for value in row)
        
            url = 'http://api.aviationstack.com/v1/flights?&access_key='+ api_key + '&dep_iata='+ str(iata_value)

            response = requests.get(url)
            data = response.json()
        
            if len(json.dumps(data)) >150:
                producer.send(topic=topicfly, value=json.dumps(data).encode('utf-8'))
                producer.flush()
                print('done ' + str(iata_value) ,datetime.now())
            time.sleep(10)
        
        mysql_conn = mc.connect(
        user=mysql_username,
        password=mysql_password,
        host=host,
        port=mysql_port,
        autocommit=True,
        database=mysql_database_name
        )

        df = pd.read_sql('SELECT dep_iata FROM airport_dep_iata where status=1', con=mysql_conn)
        mysql_conn.close()
        
        time.sleep(60)    
    except requests.exceptions.RequestException as e:
        print('Error occurred:', e)
        break