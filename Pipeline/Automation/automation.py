import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Establish a connection to the Postgres database
conn = psycopg2.connect(
    host="localhost",
    database="task",
    user="postgres",
    password="1713"
)

# Create a cursor object to interact with the database
cur = conn.cursor()

# Define the table schema
table_name = "Crypt"
cols = ["Year", "Name", "Stars", "Director"]
col_defs = [f"{col} TEXT" for col in cols]
create_table_query = f"CREATE TABLE IF NOT EXISTS public.{table_name} ({', '.join(col_defs)});"
cur.execute(create_table_query)

# Create a SQLAlchemy engine for bulk insertions
engine = create_engine("postgresql://postgres:1713@localhost:5432/task")

# Read the CSV file
file_path = "B:\Python\Games.csv"
df = pd.read_csv(file_path)

# Process the data in batches of 3 rows
batch_size = 3
num_batches = (len(df) + batch_size - 1) // batch_size

for i in range(num_batches):
    # Select the next batch of rows
    start_index = i * batch_size
    end_index = (i + 1) * batch_size
    batch_df = df.iloc[start_index:end_index]

    # Transform the data by selecting only the first 3 rows
    transformed_df = batch_df.head(3)

    # Load the transformed data into the PostgreSQL database
    transformed_df.to_sql(table_name, engine, if_exists="append", index=False)

    # Wait for 5 minutes before processing the next batch
    time.sleep(30)

# Close the database connection
cur.close()
conn.close()
