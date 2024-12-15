from dagster import asset, multi_asset, AssetExecutionContext, MetadataValue ,AssetSpec, AssetIn
import pandas as pd


@asset
def a():
    return list(range(5)) 

@asset
def b():
    return list(range(10, 20, 2)) 


@multi_asset(
        specs=[AssetSpec("c", deps=["b", "a"])],
        ins={"a_vals": AssetIn(key=["a"]), "b_vals": AssetIn(key=["b"])}
        )
def my_complex_assets(context: AssetExecutionContext, a_vals, b_vals): 
    df = pd.DataFrame({'a': a_vals, 'b': b_vals})
    return context.add_output_metadata({
       "stat": MetadataValue.md(df.describe().to_markdown()),
       "head": MetadataValue.md(df.head().to_markdown()),
    })