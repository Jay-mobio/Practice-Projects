from pyspark.sql import SparkSession
import requests
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import pandas as pd

engine = create_engine('postgresql://postgres:1713@localhost:5432/Mychat')


OWNER = 'CSSEGISandData'
REPO = 'COVID-19'
PATH = 'csse_covid_19_data/csse_covid_19_daily_reports'
URL = f'https://api.github.com/repos/{OWNER}/{REPO}/contents/{PATH}'
print(f'Downloading paths from {URL}')


file_path = 'B:\Python\Games.csv'

download_urls = []  
download_urls.append(file_path)

response = requests.get(URL)

# dowanloading and appending urls
for data in tqdm(response.json()):
    if data['name'].endswith('.csv'):
        download_urls.append(data['download_url'])


spark = SparkSession.builder.master('local[*]').appName("ETL_process").getOrCreate()

def process_dataset(dataset_path, column_map=None):
    # Load the data set
        df = pd.read_csv(dataset_path)

        # Rename columns if column_map is provided
        # if column_map:
        #     for col_name in column_map:
        #         df = df.withColumnRenamed(col_name, column_map[col_name])

        # Clean the data set
        # df = df.na.drop()
        # df = df.dropDuplicates()

        # Transform the data set
        # upper = udf(upper_case)
        # double = udf(double_value)
        # df = df.withColumn("column_a", upper(col("column_a")))
        # df = df.withColumn("column_b", double(col("column_b")))

        # Load the processed data set
        df.to_sql(name=f"{dataset_path.split('/')[-1].split('.')[0]}_processed",if_exists='replace',con=engine)
        # filename = df.write.csv(f"{dataset_path.split('/')[-1].split('.')[0]}_processed", mode='overwrite')

    # List of data sets to process

# Loop through the list of data sets and process each one
for dataset in download_urls:
    df = pd.read_csv(dataset)
    df.to_sql(name=f"{dataset.split('/')[-1].split('.')[0]}_processed",if_exists='replace',con=engine)

# Stop Spark Session
spark.stop()
