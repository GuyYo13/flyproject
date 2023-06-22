import mysql.connector as mc
import requests

# MySQL

host = 'localhost'

mysql_port = 3306

mysql_database_name = 'classicmodels' #'srcdb'

mysql_table_name = 'fly' #'src_events'

mysql_username = 'naya'

mysql_password = 'NayaPass1!'


# connector to mysql

mysql_conn = mc.connect(

user=mysql_username,

password=mysql_password,

host=host,

port=mysql_port,

autocommit=True, # <--

database=mysql_database_name)


# Create a cursor to execute SQL queries
cursor = mysql_conn.cursor()

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

values = ('Airline X', 'FL123', 'New York', 'London', '2023-05-30 10:00:00', '2023-05-30 16:00:00')
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

