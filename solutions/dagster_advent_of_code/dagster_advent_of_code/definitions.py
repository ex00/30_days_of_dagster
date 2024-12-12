from dagster import Definitions, load_assets_from_modules, PipesSubprocessClient

from dagster_advent_of_code import assets_quick_start, assets_day12 as day_asserts # noqa: TID252
    

all_assets = load_assets_from_modules([assets_quick_start, day_asserts])


defs = Definitions(
    assets=all_assets,
    sensors=[day_asserts.run_asserts_on_top_changed_data],
    resources={
        "pipes_subprocess_client": PipesSubprocessClient(),
        "raw_data": day_asserts.DataToFileResource(path=day_asserts.MyAssetConfig().raw_file_path),
        "bronze_data": day_asserts.DataToFileResource(path=day_asserts.MyAssetConfig().bronze_file_path),
    }
)
