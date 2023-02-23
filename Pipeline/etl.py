import requests
import pandas as pd
import numpy as np
import os
from tqdm.auto import tqdm
from sqlalchemy import create_engine


# Constant values
OWNER = 'CSSEGISandData'
REPO = 'COVID-19'
PATH = 'csse_covid_19_data/csse_covid_19_daily_reports'
URL = f'https://api.github.com/repos/{OWNER}/{REPO}/contents/{PATH}'
print(f'Downloading paths from {URL}')


download_urls = []

response = requests.get(URL)

# dowanloading and appending urls
for data in tqdm(response.json()):
    if data['name'].endswith('.csv'):
        download_urls.append(data['download_url'])


# list of lables renamed
relable = {
     # 'Last Update': 'Last_Update',
    'Contry/Region': 'Country_Region',
    'Lat': 'Latitude',
    'Long_': 'Longitude',
    'Province/State': 'Province_State'
}

def factor_dataframe(dat,filename):
    """Refactor the dataframe into a SQL dataframe as a pandas Dataframe
    """
    for label in dat:
        if label in relable:
            dat = dat.rename(columns = {label:relable[label]})
    
    labels = ['Province_State', 'Country_Region', 'Last_Update', 'Confirmed', 'Deaths', 'Recovered']
    if 'Last_Update' not in dat:
        dat['Last_Update'] = pd.to_datetime(filename)
    for label in labels:
        if label not in dat:
            dat[label] = np.nan

    return dat[labels]

engine = create_engine('postgresql://postgres:1713@localhost:5432/task')

def upload_to_sql(filenames, db_name, debug=False):
    for i,file_path in tqdm(list(enumerate(filenames))):
        dat = pd.read_csv(file_path)
        print(dat)

        filename = os.path.basename(file_path).split('.')[0]
        dat = factor_dataframe(dat, filename)

        # write records to sql database
        if i == 0: # if first entry, and table name already exist, replace
            dat.to_sql(name=filename,if_exists='replace', con=engine)
        else: # otherwise append to current table given db_name
            dat.to_sql(name=filename, if_exists='append', con=engine)

upload_to_sql(download_urls,engine, debug=True)
