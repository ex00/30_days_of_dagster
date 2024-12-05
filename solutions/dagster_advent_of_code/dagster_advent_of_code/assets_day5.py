from dagster import asset   
from urllib.request import urlretrieve
from pathlib import Path

import pandas as pd

DATA_URL = "https://raw.githubusercontent.com/ph4un00b/data/62caf2a6ae0e8966e50d2350cabe530a65d354db/1000movies.csv"
DATA_FOLDER = Path(__file__).parent / "data"


@asset(
        key_prefix="downloaded_data"
)
def download_data():
    DATA_FOLDER.mkdir(exist_ok=True)
    return urlretrieve(DATA_URL, DATA_FOLDER / "1000movies.csv")

@asset(
        deps=[download_data]
)
def read_data(downloaded_data):
    print(downloaded_data)
    df = pd.read_csv(DATA_FOLDER / "1000movies.csv")
    print(df)
    return df

@asset(
    deps=[read_data]
) 
def b(): ... 
