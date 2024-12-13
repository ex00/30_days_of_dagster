from dagster import asset, AssetExecutionContext, TimeWindowPartitionsDefinition ,MetadataValue 
from datetime import datetime
from urllib.request import urlretrieve
import pandas as pd
import os

DATA_URL = "https://raw.githubusercontent.com/ph4un00b/data/62caf2a6ae0e8966e50d2350cabe530a65d354db/1000movies.csv"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
days = TimeWindowPartitionsDefinition(start=datetime(2024,12,13), fmt=DATE_FORMAT, cron_schedule="*/10 * * * *")

@asset(
    partitions_def=days
) 
def download_and_process(context: AssetExecutionContext):
    partition = context.partition_key
    path = "1000movies.csv"
    if not os.path.exists(path):
        context.log.debug("Download data")
        path = urlretrieve(DATA_URL, path)[0]
    
    context.log.info(f"Processing partition {partition}")
    date = datetime.strptime(partition, DATE_FORMAT)
    df = pd.read_csv(path)
    context.log.info(f"Will process only index: {date.minute}")

    return context.add_output_metadata({
        "row_number": date.minute,
        "row_value": MetadataValue.md(df.iloc[[date.minute]].to_markdown()),
    })


