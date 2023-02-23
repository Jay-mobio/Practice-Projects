import json
import pandas as pd
import requests
from datetime import datetime
import os
from prefect import task
@task
def extract(url: str):
    res = requests.get(url)

    if not res:
        raise Exception("No data fetched")
    return json.loads(res.content)

@task
def transform(data:dict):
    transformed = []
    for user in data:
        transformed.append({
            "ID": user["id"],
            "Name": user["name"],
            "Username": user["username"],
            "Email": user["email"],
            "Address": f"{user['address']['street']}, {user['address']['suite']}, {user['address']['city']}",
            "PhoneNumber": user["phone"],
            "Company": user["company"]["name"]

        })
    return pd.DataFrame(transformed)

@task
def load(data:pd.DataFrame,path:str):
    data.to_csv(path_or_buf=path,index=False)

if __name__ == '__main__':
    users = extract(url="https://jsonplaceholder.typicode.com/users")
    df_users = transform(users)
    load(data=df_users, path=os.getcwd()+f'/users_{int(datetime.now().timestamp())}.csv')