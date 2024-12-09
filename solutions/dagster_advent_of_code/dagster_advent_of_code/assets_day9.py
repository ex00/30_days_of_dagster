from dagster import asset, MetadataValue, sensor ,AssetExecutionContext, SkipReason, SensorEvaluationContext , RunRequest, Config, RunConfig
from urllib.request import urlretrieve
from pathlib import Path
import os

import pandas as pd

DATA_URL = "https://raw.githubusercontent.com/ph4un00b/data/62caf2a6ae0e8966e50d2350cabe530a65d354db/1000movies.csv"
DATA_FOLDER = Path(__file__).parent / "data"

# class MyAssetConfig(Config):
#     data_url: str = "https://raw.githubusercontent.com/ph4un00b/data/62caf2a6ae0e8966e50d2350cabe530a65d354db/1000movies.csv"
#     data_folder: Path = Path(__file__).parent / "data"
#     raw_file : Path = data_folder / "1000movies.csv"

class MyAssetConfig(Config):
    raw_file_path : str
    bronze_file_path: str


@asset(
    kinds={'download', 'csv'},
)
def download_data_path(_context: AssetExecutionContext):
    DATA_FOLDER.mkdir(exist_ok=True)
    _context.log.debug(f"Download data from {DATA_URL} to {DATA_FOLDER}")
    return urlretrieve(DATA_URL, DATA_FOLDER / "1000movies.csv" )[0]

@asset(
        deps=[download_data_path],
        tags={'layer': 'bronze'},
        kinds={'pandas', 'csv'},
)
def read_data(context: AssetExecutionContext, config: MyAssetConfig):
    context.log.debug(f"Read download_data_path")
    df = pd.read_csv(config.raw_file_path)
    
    context.add_output_metadata({
       "rows": len(df),
       "head": MetadataValue.md(df.head().to_markdown()),
       "columns": MetadataValue.md(str(df.columns))  
    })

    df.to_csv(config.bronze_file_path, index = False)
    return df
 

@asset(
        deps=[read_data],
        kinds={'pandas', 'stat'},
)
def stat_data(context: AssetExecutionContext, config: MyAssetConfig):
    context.log.debug(f"Run stats for data")
    data = pd.read_csv(config.bronze_file_path)
    context.add_output_metadata({
       "stat": MetadataValue.md(data.describe().to_markdown()),
    })
    return data.describe()

@sensor(
    target=["read_data", "stat_data" ], # if leave read_data* then gets `TypeError: '<' not supported between instances of 'AssetsDefinition' and 'str'`, but it's not possible to put asset in RunConfig dict
    minimum_interval_seconds = 10,
)
def run_asserts_on_top_changed_data(context: SensorEvaluationContext):
    file = str(DATA_FOLDER / "1000movies.csv")
    if not os.path.exists(file):
        return SkipReason(f"File {file} doesn't exist")
    
    last_modified_time = os.path.getmtime(file)

    asset_config = MyAssetConfig(raw_file_path=file, bronze_file_path=str(DATA_FOLDER / "data.csv"))

    if not context.cursor:
        context.log.info(f"First time through so we are launching a run and updating the cursor")
        context.update_cursor(str(last_modified_time))
        return RunRequest(run_config=
                        RunConfig({"read_data": asset_config, "stat_data": asset_config})
                )
    
    if last_modified_time > float(context.cursor):
        context.log.info(f"Update cursor value: it was {str(context.cursor)}; new value is: {str(last_modified_time)}")
        context.update_cursor(str(last_modified_time))
        return RunRequest(run_config=
                        RunConfig({"read_data": asset_config, "stat_data": asset_config})
                )
    
    return SkipReason(f"The file was last updated at {str(context.cursor)}")

