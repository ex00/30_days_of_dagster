from dagster import Definitions, load_assets_from_modules, PipesSubprocessClient

from dagster_advent_of_code import assets_quick_start, assets_day13 as day_asserts # noqa: TID252
    

all_assets = load_assets_from_modules([assets_quick_start, day_asserts])


defs = Definitions(
    assets=all_assets
)
