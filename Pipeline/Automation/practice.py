# import psycopg2

# # Establish a connection to the Postgres database
# conn = psycopg2.connect(
#     host="localhost",
#     database="task",
#     user="postgres",
#     password="1713"
# )

# # Create a cursor object to interact with the database
# cur = conn.cursor()

# # Execute a SELECT query to fetch data
# cur.execute('SELECT * FROM public."Games"')
# rows = cur.fetchall()
# print(len(rows))
# # Close the cursor and connection
# cur.close()
# conn.close()

import pandas as pd

# Open the CSV file for reading
# with open('B:\Python\Games.csv', 'r') as csvfile:
    # Create a Pandas DataFrame
df = pd.read_csv('B:\Python\Games.csv')
    
    # Drop the first row (header)
df = df.drop([0])
    
    # Initialize a counter
count = 0
    
    # Initialize an empty DataFrame to store the results
result = pd.DataFrame()
    
    # Iterate over the rows in the file
for i in range(0, len(df), 3):
    # Create a DataFrame with the next three rows
    temp = df.iloc[i:i+3]
    
    # Append the DataFrame to the result
    result = result.append(temp)
    
    # Increment the counter
    count += 1
    
# Print the result
print(result)
