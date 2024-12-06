from dagster import asset ,AutomationCondition, AssetIn
from urllib.request import urlretrieve
from pathlib import Path

import pandas as pd

DATA_URL = "https://raw.githubusercontent.com/ph4un00b/data/62caf2a6ae0e8966e50d2350cabe530a65d354db/1000movies.csv"
DATA_FOLDER = Path(__file__).parent / "data"


@asset(
    automation_condition=AutomationCondition.any_downstream_conditions()
)
def download_data():
    DATA_FOLDER.mkdir(exist_ok=True)
    return urlretrieve(DATA_URL, DATA_FOLDER / "1000movies.csv")[0]

@asset(
    ins={"path_to_data": AssetIn(key=["download_data"])},
)
def read_data(path_to_data:Path):
    df = pd.read_csv(path_to_data)
    print(df)
    return df

@asset(
    deps=[read_data]
) 
def b(): ... 
