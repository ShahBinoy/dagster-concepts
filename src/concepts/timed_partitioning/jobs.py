from dagster import define_asset_job, load_assets_from_modules, AssetSelection

from .assets import (timed_weekly_inventory, timed_daily_inventory)

build_timed_daily_inventory_job = define_asset_job("build_timed_daily_inventory_job", selection=AssetSelection.assets(timed_daily_inventory))

build_timed_weekly_inventory_job = define_asset_job("build_timed_weekly_inventory_job", selection=AssetSelection.assets(timed_weekly_inventory))


