from kafka import KafkaProducer
from time import sleep
import time
import connection as con
import json

# Topics/Brokers
topicfly = 'Flytopic'
brokers = ['cnt7-naya-cdh63:9092']


producer = KafkaProducer(bootstrap_servers=brokers)

# The send() method creates the topic



# Specify the flight number you want to retrieve information for
flight_number = 'LY323'

# API endpoint URL
url = f'http://api.aviationstack.com/v1/flights?&access_key={con.api_key}&flight_iata={flight_number}'

try:
    response = con.requests.get(url)
    data = response.json()
   
    producer.send(topic=topicfly, value=json.dumps(data).encode('utf-8'))
    producer.flush()

    print(data)
except con.requests.exceptions.RequestException as e:
    print('Error occurred:', e)

