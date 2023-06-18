import json
from kafka import KafkaConsumer
import mysql.connector as mc
import time 

# Kafka Consumer settings
bootstrap_servers = ['cnt7-naya-cdh63:9092']
topic_name = 'lv1'

host = 'localhost'
mysql_port = 3306
mysql_database_name = 'classicmodels'
mysql_table_name = 'flights'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'

# Connect to Kafka
consumer = KafkaConsumer(
    topic_name,
    client_id='consumer1',
    group_id='2002',
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000
)

# Connector to MySQL
mysql_conn = mc.connect(
    user=mysql_username,
    password=mysql_password,
    host=host,
    port=mysql_port,
    autocommit=True,
    database=mysql_database_name
)
cursor = mysql_conn.cursor()

# Create the flights table if it doesn't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS flights (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    airline VARCHAR(255),
                    flight_number VARCHAR(255),
                    origin VARCHAR(255),
                    destination VARCHAR(255),
                    destination_code VARCHAR(255),
                    planned_time VARCHAR(255),
                    actual_time VARCHAR(255),
                    delay VARCHAR(255),
                    UNIQUE KEY (flight_number, planned_time)
                )''')

# Process Kafka messages and insert into MySQL
for message in consumer:
    data = message.value.decode('utf-8')  # Assuming the data is in UTF-8 encoding
    # print (data)
    try:
        # Parse the JSON data
        json_data = json.loads(data)
        if len(json.dumps(json_data)) >150:
        
            field1 = json_data['data']
            for i in field1:
                # Extract values from JSON as needed
                airline = i['airline']['name']
                flight_number = i['flight']['number']
                origin = i['departure']['airport']
                destination = i['arrival']['airport']
                destination_code = i['arrival']['iata']
                planned_time = i['departure']['scheduled']
                actual_time = i['departure']['actual']
                delay = i['departure']['delay']

                # print (airline)
                # print (flight_number)

                
                # Insert or update a row in the flights table
                insert_query = '''INSERT INTO flights
                                    (airline, flight_number, origin, destination, destination_code, planned_time, actual_time, delay)
                                VALUES
                                    (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    actual_time = (%s),
                                    delay = (%s)'''

                values = (airline, flight_number, origin, destination, destination_code, planned_time, actual_time, delay,
                        actual_time, delay)
                cursor.execute(insert_query, values)

            # Commit the changes to the database
                mysql_conn.commit()
    except Exception as e:
        print("Error occurred while processing message:", e)

# Close the cursor and connection
cursor.close()
mysql_conn.close()
