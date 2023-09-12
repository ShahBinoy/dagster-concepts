import os
from typing import List, Dict
from datetime import datetime, timedelta

import polars as pl

from dagster import (DailyPartitionsDefinition,
                     MultiPartitionsDefinition,
                     WeeklyPartitionsDefinition,
                     asset,
                     Config,
                     MultiToSingleDimensionPartitionMapping,
                     AssetIn,
                     asset_sensor,
                     SensorEvaluationContext,
                     EventLogEntry,
                     RunRequest,
                     RunConfig,
                     define_asset_job,
                     AssetSelection,
                     DefaultSensorStatus,
                     AssetOut,
                     Output,
                     SkipReason
                     )

start_date = datetime.strptime(os.getenv("DATALAKE_START_DATE", '2023-08-01'), '%Y-%m-%d')

daily_partition = DailyPartitionsDefinition(start_date=start_date)

weekly = WeeklyPartitionsDefinition(start_date=start_date)


class MyConfig(Config):
    state_id: str = "Hello"
    full_refresh: bool = False
    max_batch_size = 5


class MyConfig2(Config):
    prop2: str = "Worlds"

from dagster import PartitionKeyRange

@asset(
    partitions_def=daily_partition,
    group_name="datalake_core_timed"
)
def timed_daily_inventory(context, config: MyConfig) -> Output[pl.DataFrame]:
    partition_date_str = context.asset_partition_key_for_output()
    mult_days:PartitionKeyRange = context.asset_partition_key_range_for_output()

    print(f"Daily Inventory With Partition is {partition_date_str} for config {config.state_id}")
    partition_date_str = os.getenv("FANNING_START_DATE", '2023-07-04')
    dt = datetime.strptime(partition_date_str, '%Y-%m-%d')
    prev_dt = dt - timedelta(days=2)
    prev_dt2 = prev_dt - timedelta(days=2)
    data = [
        pl.Series("lastModifiedDate", [dt, dt, prev_dt, prev_dt, prev_dt, prev_dt2, prev_dt2], dtype=pl.Date),
        pl.Series("size", [333, 4444, 9374, 89237, 3884, 475, 2727], dtype=pl.Int64),
    ]
    inventory_df = pl.DataFrame(data)
    return Output(value=inventory_df, metadata={"keyMeta": "valMeta"})


@asset(
    partitions_def=weekly,
    group_name="datalake_core_timed"
)
def timed_weekly_inventory(context, config: MyConfig, timed_daily_inventory):
    partition_date_str = context.asset_partition_key_for_output()
    print(f"Weekly Inventory with Partition is {partition_date_str} for config {config.state_id}")

    invys: Dict = timed_daily_inventory
    print(invys.keys())
    timed_daily_inventory_df = timed_daily_inventory[partition_date_str]
    # print(timed_daily_inventory)
    chunked_df = timed_daily_inventory_df.select(pl.all(),
                                                 pl.col("size").alias("CumSize").cast(pl.Int64).cumsum()
                                                 ).with_columns(pl.all(),
                                                                pl.col("CumSize").alias(
                                                                    "batch_num") // config.max_batch_size
                                                                ).partition_by("batch_num", as_dict=True)
    return chunked_df


split_inventory_job = define_asset_job("weekly_inventory_job", selection=AssetSelection.assets(timed_weekly_inventory))


@asset_sensor(asset_key=timed_daily_inventory.key,
              job=split_inventory_job,
              default_status=DefaultSensorStatus.RUNNING)
def date_time_inventory_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    assert asset_event.dagster_event and asset_event.dagster_event.asset_key
    print(asset_event.dagster_event.asset_key)
    yield SkipReason(skip_message="meh !!")
