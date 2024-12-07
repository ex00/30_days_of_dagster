from dagster import asset, MetadataValue, AssetIn ,AssetExecutionContext
from urllib.request import urlretrieve
from pathlib import Path

import pandas as pd

DATA_URL = "https://raw.githubusercontent.com/ph4un00b/data/62caf2a6ae0e8966e50d2350cabe530a65d354db/1000movies.csv"
DATA_FOLDER = Path(__file__).parent / "data"


@asset(
    kinds={'download', 'csv'},
)
def download_data_path(_context: AssetExecutionContext):
    DATA_FOLDER.mkdir(exist_ok=True)
    _context.log.debug(f"Download data from {DATA_URL} to {DATA_FOLDER}")
    return urlretrieve(DATA_URL, DATA_FOLDER / "1000movies.csv" )[0]

@asset(
        tags={'layer': 'bronze'},
        kinds={'pandas', 'csv'},
)
def read_data(context: AssetExecutionContext, download_data_path):
    context.log.debug(f"Read download_data_path")
    df = pd.read_csv(download_data_path)
    
    context.add_output_metadata({
       "rows": len(df),
       "head": MetadataValue.md(df.head().to_markdown()),
       "columns": MetadataValue.md(str(df.columns))  
    })

    df.to_csv(DATA_FOLDER / "data.csv", index = False)
    return df
 

@asset(
        deps=[read_data],
        kinds={'pandas', 'stat'},
)
def stat_data(context: AssetExecutionContext):
    context.log.debug(f"Run stats for data")
    data = pd.read_csv(DATA_FOLDER / "data.csv")
    context.add_output_metadata({
       "stat": MetadataValue.md(data.describe().to_markdown()),
    })
    return data.describe()
