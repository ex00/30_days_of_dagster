from dagster import asset, MetadataValue, AssetIn ,AssetExecutionContext
from urllib.request import urlretrieve
from pathlib import Path

import pandas as pd

DATA_URL = "https://raw.githubusercontent.com/ph4un00b/data/62caf2a6ae0e8966e50d2350cabe530a65d354db/1000movies.csv"
DATA_FOLDER = Path(__file__).parent / "data"
DATA = DATA_FOLDER / "1000movies.csv"


@asset(
    kinds={'download', 'csv'},
)
def download_data(_context: AssetExecutionContext):
    DATA_FOLDER.mkdir(exist_ok=True)
    _context.log.debug(f"Download data from {DATA_URL} to {DATA_FOLDER}")
    return urlretrieve(DATA_URL, DATA)[0]

@asset(
        deps=[download_data],
        tags={'layer': 'bronze'},
        kinds={'pandas', 'csv'},
)
def read_data(context: AssetExecutionContext):
    context.log.debug(f"Read {DATA}")
    df = pd.read_csv(DATA)
    
    context.add_output_metadata({
       "rows": len(df),
       "head": MetadataValue.md(df.head().to_markdown()),
       "columns": MetadataValue.md(str(df.columns))  
    })
 
    return df

@asset(
        deps=[read_data],
        kinds={'pandas', 'stat'},
)
def stat_data(context: AssetExecutionContext):
    context.log.debug(f"Return stats for data")
    df = pd.read_csv(DATA)
    context.add_output_metadata({
       "stat": MetadataValue.md(df.describe().to_markdown()),
    })
    return df.describe()
