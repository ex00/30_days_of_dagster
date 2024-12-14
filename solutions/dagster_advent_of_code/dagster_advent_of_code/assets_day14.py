from dagster import asset, AssetExecutionContext, AutomationCondition
import random

@asset
def asset_a(): 
    val = random.randint(1,4)
    if val > 1:
        return val
    return 0

def asset_factory(asset_name):
    @asset(
            name=asset_name
    ) 
    def dynamic_asset(context: AssetExecutionContext,asset_a): 
        context.log.info(f"Hello from {asset_name}, value of a is :{asset_a}")

    return dynamic_asset

dynamic_assets = [asset_factory(str(i)) for i in range(1,10)]