import pandas as pd
from sqlalchemy import create_engine

# Set up the database connection
engine = create_engine('postgresql://postgres:1713@localhost:5432/task')

# Read the CSV file into a pandas DataFrame and specify column names
df = pd.read_csv('B:\Python\data.csv', names=['first_name', 'last_name', 'age'])

# Write the DataFrame to the database table
df.to_sql('Members', engine, if_exists='replace', index=False)
