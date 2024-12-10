from dagster import Definitions, load_assets_from_modules

from dagster_advent_of_code import assets_quick_start, assets_day10 # noqa: TID252
    

all_assets = load_assets_from_modules([assets_quick_start, assets_day10])


defs = Definitions(
    assets=all_assets,
    sensors=[assets_day10.run_asserts_on_top_changed_data],
    resources={
        "raw_data": assets_day10.DataToFileResource(path=assets_day10.MyAssetConfig().raw_file_path),
        "bronze_data": assets_day10.DataToFileResource(path=assets_day10.MyAssetConfig().bronze_file_path),
    }
)
