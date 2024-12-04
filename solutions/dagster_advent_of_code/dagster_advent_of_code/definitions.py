from dagster import Definitions, load_assets_from_modules

from dagster_advent_of_code import assets_quick_start, assets_day4 # noqa: TID252
    

all_assets = load_assets_from_modules([assets_quick_start, assets_day4])


defs = Definitions(
    assets=all_assets
)
