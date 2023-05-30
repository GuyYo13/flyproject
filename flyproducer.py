from kafka import KafkaProducer
from time import sleep
import time

# Topics/Brokers
topicfly = 'Flytopic'
brokers = ['cnt7-naya-cdh63:9092']


producer = KafkaProducer(bootstrap_servers=brokers)

# The send() method creates the topic

