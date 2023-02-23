import requests
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine

relabel = {
    'Title':'Name',
    'rate':'Stars',
    'Release': 'Year'
}
# Title,rate,Year,certificate,genre,rate,story,director,votes count
def factor_dataframe(dat):
    for label in dat:
        if label in relabel:
            dat = dat.rename(columns = {label:relabel[label]})

    new_df = dat.drop(columns=['certificate','genre'])
    labels = ['Year','Name','Stars','director','votes count']

    return new_df[labels]

engine = create_engine('postgresql://postgres:1713@localhost:5432/task')

def upload_to_postgres(file_path,db_name,debug=False):
    dat = pd.read_csv(file_path,skiprows=3,nrows=3)
    print(dat)
    dat = factor_dataframe(dat)
    dat.to_sql(name='Games',if_exists='replace',con=engine)
file_path = 'B:\Python\Games.csv'
upload_to_postgres(file_path, engine)
# B:\Python\2022 Games.csv