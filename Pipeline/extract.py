import pandas as pd
import requests
import json
import os
import  io

class Extract:

    def __init__(self):
        self.data_sources = json.load(open(os.getcwd()+'\config.json'))
        self.api = self.data_sources['data_sources']['api']
        self.csv_url = self.data_sources['data_sources']['csv']
        self.file_name = self.data_sources['data_sources']['file_csv']

    def getAPISData(self,api_name):
        api_url = self.api[api_name]
        response = requests.get(api_url)
        return response.json()

    def getCSVData(self,csv_name):
        csv_url = self.csv_url[csv_name]
        response = requests.get(csv_url).content
        df = pd.read_csv(io.StringIO(response.decode('utf-8')))
        return df
    
    def getfileCsvData(self,file_name):
        csv_file = "B:\Python\Hotel Reservations.csv"
        df = pd.read_csv(csv_file)
        return df
