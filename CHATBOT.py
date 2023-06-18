import mysql.connector
import re

# Connect to the MySQL server
connection = mysql.connector.connect(
    host='localhost',
    port=3306,
    database='classicmodels',
    user='naya',
    password='NayaPass1!'
)

if connection.is_connected():
    print("Connected to MySQL database")

    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # Prompt the user for their query
    query = input("Enter your query: ")

    # Extract flight number and date from the query with regular expressions
    flight_number = re.search(r'\bflight number (\d+)', query, flags=re.IGNORECASE)
    flight_date = re.search(r'\b(\d{4}-\d{2}-\d{2})', query)

    if flight_number and flight_date:
        flight_number = flight_number.group(1)
        flight_date = flight_date.group(1)

        
        query = "SELECT delay, actual_time, planned_time FROM flights WHERE flight_number = %s AND DATE(planned_time) = %s"
        cursor.execute(query, (flight_number, flight_date))

       
        row = cursor.fetchone()

        if row:
            delay = row[0]
            actual_time = row[1]
            planned_time = row[2]

            if delay:
                if actual_time:
                    print(f"There is a delay of {delay} minutes on your flight. The actual time is {actual_time}.")
                else:
                    print(f"There is a delay of {delay} minutes on your flight. The actual time is {planned_time} or later.")
            else:
                print("There is no delay on your flight.")
        else:
            print("Flight details not found.")
    else:
        print("Could not understand the query.")

    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("Connection closed")
else:
    print("Failed to connect to MySQL database")
