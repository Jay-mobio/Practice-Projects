import psycopg2
import pandas as pd

# PostgreSQL connection details
host = 'localhost'
port = '5432'
dbname = 'task'
user = 'postgres'
password = '1713'

# CSV file details
csv_file = 'B:\Python\crypto-market-GBP.csv'
table_name = 'Cryptoo'

# Load data from CSV file using pandas
df = pd.read_csv(csv_file)

# Get the column names from the CSV file
cols = list(df.columns)
df = df.rename(columns={'Unnamed: 0': 'id'})
cols[0] = df.columns[0]
df.columns = [col.replace(" ", "_") for col in df.columns]
df.columns = [col.replace(".", "_") for col in df.columns]
# Create a connection to PostgreSQL
conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
cur = conn.cursor()

# Create the table if it doesn't already exist, with columns matching the CSV file
cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} VARCHAR(255)' for col in cols])})")
cur.execute(f"SELECT COUNT(*) FROM {table_name}")
num_rows = cur.fetchone()[0]
df = pd.read_csv(csv_file, skiprows=num_rows, nrows=3)
# Insert data into PostgreSQL table
for row in df.itertuples(index=False):
    cur.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})", row)

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()
