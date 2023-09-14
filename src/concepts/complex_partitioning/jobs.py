from dagster import define_asset_job, load_assets_from_modules, AssetSelection

from .assets import (inventory, coalesce_items)

build_inventory_job = define_asset_job("build_inventory_job", selection=AssetSelection.assets(inventory))

coalesce_inventory_job = define_asset_job("coalesce_inventory_job",
                                          selection=AssetSelection.assets(coalesce_items))
