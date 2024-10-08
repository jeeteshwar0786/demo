import pandas as pd
import random
from datetime import datetime, timedelta
import psycopg2

# Function to generate random dates between start_date and end_date
def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

# Date range for crash_date and upload_date
start_date = datetime(2020, 1, 1)
end_date = datetime(2023, 12, 31)

# Random sample data
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Martinez", "Hernandez"]
streets = ["Maple St", "Oak St", "Pine St", "Main St", "Cedar Ave", "Elm St", "Birch Ln", "Walnut Ave", "Willow Dr", "Cherry Rd"]

# Generate 100 rows of random data
data = {
    'rep_num': [random.randint(1000, 9999) for _ in range(100)],
    'street': [random.choice(streets) for _ in range(100)],
    'crash_date': [random_date(start_date, end_date).strftime('%Y-%m-%d') for _ in range(100)],
    'last_name': [random.choice(last_names) for _ in range(100)],
    'upload_date': [random_date(start_date, end_date).strftime('%Y-%m-%d') for _ in range(100)]
}

# Create DataFrame
df = pd.DataFrame(data)

# Database connection details
conn = psycopg2.connect(
    dbname="demo_database",  # Replace with your database name
    user="postgres",          # Replace with your database username
    password="123456",      # Replace with your database password
    host="localhost",              # Adjust if your database is on a different host
    port="5432"                    # Default PostgreSQL port
)

# Open a cursor to perform database operations
cur = conn.cursor()

# Create a table (if not already created)
cur.execute("""
    CREATE TABLE IF NOT EXISTS crash_data (
        rep_num INT,
        street TEXT,
        crash_date DATE,
        last_name VARCHAR(50),
        upload_date DATE
    );
""")

# Insert data into the table
for index, row in df.iterrows():
    cur.execute("""
        INSERT INTO crash_data (rep_num, street, crash_date, last_name, upload_date)
        VALUES (%s, %s, %s, %s, %s);
    """, (row['rep_num'], row['street'], row['crash_date'], row['last_name'], row['upload_date']))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Data inserted successfully!")

