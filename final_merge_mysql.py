import json
from kafka import KafkaConsumer
import mysql.connector as mc

# Kafka Consumer settings
bootstrap_servers = ['cnt7-naya-cdh63:9092']
topic_name = 'Flytopic'

host = 'localhost'
mysql_port = 3306
mysql_database_name = 'classicmodels'
mysql_table_name = 'fly'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'

# Connect to Kafka
consumer = KafkaConsumer(
    topic_name,
    client_id='consumer1',
    group_id='2',
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

# Process Kafka messages and insert into MySQL
for message in consumer:
    data = message.value.decode('utf-8')  # Assuming the data is in UTF-8 encoding

    try:
        # Parse the JSON data
        json_data = json.loads(data)

        # Extract values from JSON as needed
        airline = json_data['data'][0]['airline']['name']
        flight_number = json_data['data'][0]['flight']['number']
        origin = json_data['data'][0]['departure']['airport']
        destination = json_data['data'][0]['arrival']['airport']
        departure_time = json_data['data'][0]['departure']['scheduled']
        arrival_time = json_data['data'][0]['arrival']['scheduled']

        # Create the flights table if it doesn't exist
        cursor.execute('''CREATE TABLE IF NOT EXISTS flights (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            airline VARCHAR(255),
                            flight_number VARCHAR(255),
                            origin VARCHAR(255),
                            destination VARCHAR(255),
                            departure_time VARCHAR(255),
                            arrival_time VARCHAR(255)
                        )''')

        # Merge the flight data into the flights table
        merge_query = '''MERGE INTO flights AS target
                         USING (SELECT %s AS flight_number, %s AS departure_time) AS source
                         ON target.flight_number = source.flight_number AND target.departure_time = source.departure_time
                         WHEN MATCHED THEN
                             UPDATE SET target.arrival_time = %s
                         WHEN NOT MATCHED THEN
                             INSERT (airline, flight_number, origin, destination, departure_time, arrival_time)
                             VALUES (%s, %s, %s, %s, %s, %s)'''

        values = (flight_number, departure_time, arrival_time, airline, flight_number, origin, destination, departure_time, arrival_time)
       # print (merge_query)
        cursor.execute(merge_query, values)

        # Commit the changes to the database
        mysql_conn.commit()
    except Exception as e:
        print("Error occurred while processing message:", e)

    # Select all rows from the flights table
    select_query = '''SELECT * FROM flights'''
    cursor.execute(select_query)

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data
    print("Fetched data from MySQL:")
    # for row in rows:
       # print("ID:", row[0])
       # print("Airline:", row[1])
       # print("Flight Number:", row[2])
       # print("Origin:", row[3])
       # print("Destination:", row[4])
       # print("Departure Time:", row[5])
       # print("Arrival Time:", row[6])
       # print()

# Close the cursor and connection
cursor.close()
mysql_conn.close()
