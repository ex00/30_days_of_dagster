from dagster import asset, MetadataValue, sensor, \
    AssetExecutionContext, SkipReason, SensorEvaluationContext, \
    RunRequest, Config, asset_check, \
    ConfigurableResource, AssetCheckResult, AssetCheckSeverity
from urllib.request import urlretrieve
from pathlib import Path
from pydantic import Field
import os

import pandas as pd

DATA_URL = "https://raw.githubusercontent.com/ph4un00b/data/62caf2a6ae0e8966e50d2350cabe530a65d354db/1000movies.csv"
DATA_FOLDER = Path(__file__).parent / "data"

# class MyAssetConfig(Config):
#     data_url: str = "https://raw.githubusercontent.com/ph4un00b/data/62caf2a6ae0e8966e50d2350cabe530a65d354db/1000movies.csv"
#     data_folder: Path = Path(__file__).parent / "data"
#     raw_file : Path = data_folder / "1000movies.csv"

class MyAssetConfig(Config):
    raw_file_path : str = Field(default_factory=lambda: str(DATA_FOLDER / "1000movies.csv"))
    bronze_file_path: str =  Field(default_factory=lambda: str(DATA_FOLDER / "data.csv"))

class DataToFileResource(ConfigurableResource):
    path: str 

    def write(self, data: pd.DataFrame):
        data.to_csv(self.path, index=False)

    def read(self) -> pd.DataFrame: 
        return pd.read_csv(self.path)


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
def read_data(context: AssetExecutionContext, raw_data: DataToFileResource, bronze_data: DataToFileResource):
    context.log.debug(f"Read download_data_path")
    df = raw_data.read()
    
    context.add_output_metadata({
       "rows": len(df),
       "head": MetadataValue.md(df.head().to_markdown()),
       "columns": MetadataValue.md(str(df.columns))  
    })

    bronze_data.write(df)
    return df
 

@asset_check(
        asset=read_data,
        blocking=True
)
def check_read_data(bronze_data: DataToFileResource):
    df = bronze_data.read()

    return AssetCheckResult(
        severity=AssetCheckSeverity.ERROR,
        passed=len(df) >= 1000,
        metadata={"rows": len(df)},
        description = "Data too short for processing"
    )

@asset(
        deps=[read_data],
        kinds={'pandas', 'stat'},
)
def stat_data(context: AssetExecutionContext, bronze_data: DataToFileResource):
    context.log.debug(f"Run stats for data")
    data = bronze_data.read()
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


    if not context.cursor:
        context.log.info(f"First time through so we are launching a run and updating the cursor")
        context.update_cursor(str(last_modified_time))
        return RunRequest()
    
    if last_modified_time > float(context.cursor):
        context.log.info(f"Update cursor value: it was {str(context.cursor)}; new value is: {str(last_modified_time)}")
        context.update_cursor(str(last_modified_time))
        return RunRequest()
    
    return SkipReason(f"The file was last updated at {str(context.cursor)}")

