from dagster import Definitions, load_assets_from_modules

from dagster_advent_of_code import assets_quick_start, assets_day18 as day_assets # noqa: TID252
    

all_assets = load_assets_from_modules([assets_quick_start, day_assets])



defs = Definitions(
    assets=all_assets,
    schedules=[day_assets.schedule]
)
