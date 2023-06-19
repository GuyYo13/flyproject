
import json
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime

# In this example we will illustrate a simple producer-consumer integration
topic = 'lv1'
brokers = ['cnt7-naya-cdh63:9092']

# First we set the consumer, and we use the KafkaConsumer class to create a generator of the messages.


flights = KafkaConsumer(
    topic,
    client_id='consumer1',
    group_id='55',
    bootstrap_servers=brokers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)




for message in flights:
    try:
        # Parse the JSON message
        json_data = json.loads(message.value)

        # Access and process the JSON data as needed
        # Example: Print the parsed JSON data
        #print(json_data)
        
        # Example: Access specific fields in the JSON data
        if len(json.dumps(json_data)) >150:
            field1 = json_data['data']
            for i in field1:
                print(i['flight_date'])
                print(i['flight_status'])
            
                if i.get("departure") is not None:
                    b=i['departure']
                    print(b['airport'])
                    print(b['scheduled'])
                    print(b['estimated'])
                    print(b['actual'])
                    print(b['delay'])
                    print(b['actual'])
                if i.get("arrival") is not None:
                    b=i['arrival']            
                    print(f"arrival: {b['airport']}")
                    print(b['scheduled'])
                    print(b['estimated'])
                    print(b['actual'])
                    print(b['delay'])
        # field2 = json_data['data']
            field2="hhhhhhhhhhhhhhhhhhhhhhhhhhhhh"
            #print(f"Field 1: {field1}, Field 2: {field2}")

    except json.JSONDecodeError as e:
        print('Error decoding JSON:', e)

    
