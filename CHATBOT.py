import tkinter as tk
from PIL import ImageTk, Image
import mysql.connector
import re

def execute_query():
    # Get the user input from the entry widget
    query = entry.get()

    # Connect to the MySQL server
    connection = mysql.connector.connect(
        host='35.208.44.9',
        port=3306,
        database='classicmodels',
        user='chat',
        password='NayaPass1!'
    )

    if connection.is_connected():
        # Create a cursor object to execute SQL queries
        cursor = connection.cursor()

        # Extract flight number and date from the query with regular expressions
        flight_number = re.search(r'\bflight number (\d+)', query, flags=re.IGNORECASE)
        flight_date = re.search(r'\b(\d{4}-\d{2}-\d{2})', query)

        if flight_number and flight_date:
            flight_number = flight_number.group(1)
            flight_date = flight_date.group(1)

            sql_query = "SELECT delay, actual_time, planned_time FROM flights WHERE flight_number = %s AND DATE(planned_time) = %s"
            cursor.execute(sql_query, (flight_number, flight_date))

            row = cursor.fetchone()

            if row:
                delay = row[0]
                actual_time = row[1]
                planned_time = row[2]

                if delay:
                    if actual_time:
                        result_label.config(text=f"There is a delay of {delay} minutes on your flight. The actual time is {actual_time}.")
                    else:
                        result_label.config(text=f"There is a delay of {delay} minutes on your flight. The actual time is {planned_time} or later.")
                else:
                    result_label.config(text="There is no delay on your flight.")
            else:
                result_label.config(text="Flight details not found.")
        else:
            result_label.config(text="Could not understand the query.")

        # Close the cursor and connection
        cursor.close()
        connection.close()
    else:
        result_label.config(text="Failed to connect to MySQL database")

window = tk.Tk()
window.geometry("500x200")  # Set the window size

# Load the airplane image
image = Image.open("airplane1.jfif")
image = image.resize((500, 200), Image.LANCZOS)
background_image = ImageTk.PhotoImage(image)

# Create a label with the airplane image as the background
background_label = tk.Label(window, image=background_image)
background_label.place(x=0, y=0, relwidth=1, relheight=1)

# Set the default message in the input field
label = tk.Label(window, text="What do you want to know about your flight?")
label.pack(pady=10)

default_message = ""
entry = tk.Entry(window, font=("Arial", 12), width=40)
entry.insert(0, default_message)
entry.pack(pady=10)

# Remove the default message when the input field is clicked
def clear_entry(event):
    if entry.get() == default_message:
        entry.delete(0, tk.END)

entry.bind("<Button-1>", clear_entry)

# Create a button to execute the query
button = tk.Button(window, text="Bon Voyage", command=execute_query, font=("Arial", 12))
button.pack(pady=10)

# Create a label to display the result
result_label = tk.Label(window, font=("Arial", 12), wraplength=400)
result_label.pack()

window.mainloop()
