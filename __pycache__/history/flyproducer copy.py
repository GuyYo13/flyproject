import time
import kafka
import json
import requests
from kafka import KafkaProducer
from time import sleep
from datetime import datetime

# Topics/Brokers
topicfly = 'lv1'
brokers = ['cnt7-naya-cdh63:9092']

api_key = 'cf12ce3fd6fd9263a13999c7144b13be'

producer = KafkaProducer(bootstrap_servers=brokers)


# Specify the flight number you want to retrieve information for
flight_number = 'FZ1210'
dep_iata="TLV"
dep_icao="EGLL"
dep_iataLA="LAX"
# API endpoint URL

url = 'http://api.aviationstack.com/v1/flights?&access_key='+ api_key + '&dep_iata='+ dep_iata
url2 = 'http://api.aviationstack.com/v1/flights?&access_key='+ api_key + '&dep_icao='+ dep_icao
url3 = 'http://api.aviationstack.com/v1/flights?&access_key='+ api_key + '&dep_iata='+ dep_iataLA

while True:
    try:
        response = requests.get(url3)
        data = response.json()
        
        if len(json.dumps(data)) >150:
            producer.send(topic=topicfly, value=json.dumps(data).encode('utf-8'))
            producer.flush()
            print('done LA',datetime.now())

        time.sleep(300)
        response = requests.get(url)
        data = response.json()
        
        if len(json.dumps(data)) >150:
            producer.send(topic=topicfly, value=json.dumps(data).encode('utf-8'))
            producer.flush()
            print('done TLV',datetime.now())

        time.sleep(300)
        response = requests.get(url2)
        data = response.json()
        if len(json.dumps(data)) >50:
            producer.send(topic=topicfly, value=json.dumps(data).encode('utf-8'))
            producer.flush()
            print('done LON',datetime.now())
        time.sleep(300)
        
    except requests.exceptions.RequestException as e:
        print('Error occurred:', e)
        break
        

