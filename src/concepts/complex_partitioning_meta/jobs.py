from dagster import define_asset_job, load_assets_from_modules, AssetSelection

from .assets import (inventory, split_inventory)

build_inventory_job = define_asset_job("build_inventory_job", selection=AssetSelection.assets(inventory))

split_inventory_job = define_asset_job("split_inventory_job", selection=AssetSelection.assets(split_inventory))
