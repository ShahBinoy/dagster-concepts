import warnings
from typing import List

from dagster import Definitions, load_assets_from_modules, JobDefinition, ExperimentalWarning
from dagster_duckdb import build_duckdb_io_manager
from dagster_duckdb_pandas import DuckDBPandasTypeHandler
from .complex_partitioning import assets as complex_partition_assets
from .complex_partitioning.assets import inventory, coalesce_items, items_for_tenant
from .complex_partitioning.jobs import build_inventory_job, coalesce_inventory_job
from .complex_partitioning.sensor import split_inventory_sensor
from .dynamic_partitioning import assets as dynamic_partition_assets
from .complex_partitioning_meta import assets as complex_partition_meta_assets
from .dynamic_partitioning.release_sensor import release_sensor as dynamic_release_sensor
from .timed_partitioning import assets as timed_assets
from .timed_partitioning.jobs import build_timed_daily_inventory_job, build_timed_weekly_inventory_job
warnings.filterwarnings("ignore", category=ExperimentalWarning)

duckdb_io_manager = build_duckdb_io_manager([DuckDBPandasTypeHandler()])

assets_arr = []
assets_arr += load_assets_from_modules([complex_partition_assets])
assets_arr += load_assets_from_modules([dynamic_partition_assets])
assets_arr += load_assets_from_modules([timed_assets])

jobs_arr: List[JobDefinition] = list()

jobs_arr.append(build_timed_weekly_inventory_job)
jobs_arr.append(build_timed_daily_inventory_job)
jobs_arr.append(build_inventory_job)
jobs_arr.append(coalesce_inventory_job)

defs = Definitions(
    jobs=jobs_arr,
    assets=assets_arr,
    sensors=[dynamic_release_sensor, split_inventory_sensor],
    resources={"warehouse": duckdb_io_manager.configured({"database": "releases.duckdb"})},
)
