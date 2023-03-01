from sqlalchemy import text
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

engine = create_engine('postgresql://postgres:1713@localhost:5432/task')
conn = psycopg2.connect(
    host="localhost",
    database="task",
    user="postgres",
    password="1713"
)

df = pd.read_csv(r"B:\Python\updated_data.csv",names=['first_name', 'last_name', 'age'])
df.to_sql('temp', con=engine,if_exists="replace")

# Define the upsert query
query = text("""
    INSERT INTO public."Members" (first_name, last_name, age)
    SELECT first_name, last_name, age
    FROM public."temp"
    ON CONFLICT (first_name,last_name) DO UPDATE SET
      age = EXCLUDED.age;
""")

cur = conn.cursor()
cur.execute(f"""
    INSERT INTO public."Members" (first_name, last_name, age)
    SELECT first_name, last_name, age
    FROM public."temp"
    ON CONFLICT (first_name,last_name) DO UPDATE SET
      age = EXCLUDED.age;
""")
cur.execute('DROP TABLE public."temp";')
conn.commit()
conn.close()
