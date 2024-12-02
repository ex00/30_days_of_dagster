from dagster import Definitions, load_assets_from_modules

from dagster_advent_of_code import assets_quick_start, assets_day2, schedules_day2 # noqa: TID252
    

all_assets = load_assets_from_modules([assets_quick_start, assets_day2])


defs = Definitions(
    assets=all_assets,
    schedules=[schedules_day2.day2_schedule_definition, schedules_day2.day2_schedule_definition_second]
)
