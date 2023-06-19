import pandas as pd
import mysql.connector

# MySQL connection configuration
config = {
    'user': 'naya',
    'password': 'NayaPass1!',
    'host': 'localhost',
    'port': 3306,
    'database': 'classicmodels',
    'charset': 'latin1'
}

# CSV file path
csv_file = 'airportslocation.csv'

# Create a connection to MySQL
conn = mysql.connector.connect(**config)
cursor = conn.cursor()

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# MySQL table name
mysql_table_name = 'airportslocation'

# Iterate over each row in the DataFrame and insert into MySQL
for _, row in df.iterrows():
    # Replace nan values with None
    row = row.where(pd.notnull(row), None)

    # Extract the values from the row
    icao = row['icao']
    iata = row['iata']
    name = row['name']
    city = row['city']
    subd = row['subd']
    country = row['country']
    elevation = row['elevation']
    lat = row['lat']
    lon = row['lon']
    tz = row['tz']
    lid = row['lid']

    # Set empty string if lid is None
    id = '' if lid is None else lid

    # SQL query to insert the values into the table
    sql = f"INSERT INTO {mysql_table_name} (icao, iata, name, city, subd, country, elevation, lat, lon, tz, lid) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # Execute the SQL query with the parameter values
    values = (icao, iata, name, city, subd, country, elevation, lat, lon, tz, id)

    # Print values and execute the SQL query
    print(f"Inserting values: {values}")
    try:
        cursor.execute(sql, values)
    except Exception as e:
        print(f"Error executing SQL query: {e}")

# Commit the changes and close the connection
conn.commit()
conn.close()

print('Import completed successfully.')
