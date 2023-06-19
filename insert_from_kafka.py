from kafka import KafkaConsumer
import mysql.connector as mc

# Kafka Consumer settings
bootstrap_servers = ['cnt7-naya-cdh63:9092']
topic_name = 'Flytopic'
 
host = 'localhost'
mysql_port = 3306
mysql_database_name = 'classicmodels' #'srcdb'
mysql_table_name = 'fly' #'src_events'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'

# Connect to Kafka
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers)

flights = KafkaConsumer(
    topic_name,
    client_id='consumer1',
    group_id='44',
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)

# connector to mysql
mysql_conn = mc.connect(
user=mysql_username,
password=mysql_password,
host=host,
port=mysql_port,
autocommit=True, # <--
database=mysql_database_name
)
cursor = mysql_conn.cursor()

# Process Kafka messages and insert into MySQL
for message in flights:
    data = message.value.decode('utf-8')  # Assuming the data is in UTF-8 encoding
    

    
    # Split the data assuming it's comma-separated
    values = data.split(',')
    # Extract values as needed
    col1 = values[0]
    col2 = values[1]
    col3 = values[2]
    col4 = values[3]
    col5 = values[4]
    col6 = values[5]


# Create the flights table
cursor.execute('''CREATE TABLE IF NOT EXISTS flights (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    airline VARCHAR(255),
                    flight_number VARCHAR(255),
                    origin VARCHAR(255),
                    destination VARCHAR(255),
                    departure_time VARCHAR(255),
                    arrival_time VARCHAR(255)
                )''')

# Insert a row into the flights table
insert_query = '''INSERT INTO flights
                    (airline, flight_number, origin, destination, departure_time, arrival_time)
                VALUES
                    (%s, %s, %s, %s, %s, %s)'''

values = (col1, col2, col3,col4, col5, col6)
cursor.execute(insert_query, values)

# Commit the changes to the database
mysql_conn.commit()


# Select all rows from the flights table
select_query = '''SELECT * FROM flights'''
cursor.execute(select_query)

# Fetch all rows from the result set
rows = cursor.fetchall()

# Display the fetched data
for row in rows:
    print("Flight ID:", row[0])
    print("Airline:", row[1])
    print("Flight Number:", row[2])
    print("Origin:", row[3])
    print("Destination:", row[4])
    print("Departure Time:", row[5])
    print("Arrival Time:", row[6])
    print()

# Close the cursor and connection
cursor.close()
mysql_conn.close()

# #######
