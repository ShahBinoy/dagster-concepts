import os
from typing import List
from datetime import datetime, timedelta

import polars as pl

from dagster import (DailyPartitionsDefinition,
                     MultiPartitionsDefinition,
                     DynamicPartitionsDefinition,
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
                     Output
                     )

start_date = datetime.strptime(os.getenv("DATALAKE_START_DATE", '2023-07-07'), '%Y-%m-%d')

daily_partition = DailyPartitionsDefinition(start_date=start_date)

dynamic_chunks_partition = DynamicPartitionsDefinition(name="chunks")

daily_chunks_multi_part = MultiPartitionsDefinition(
    partitions_defs={"date": daily_partition, "chunks": dynamic_chunks_partition})

daily_to_chunks = MultiToSingleDimensionPartitionMapping(partition_dimension_name="date")

class ReadMaterializationConfig(Config):
    asset_key: List[str]

class MyConfig(Config):
    prop1: str = "Hello"


class MyConfig2(Config):
    prop2: str = "Worlds"


@asset(
    partitions_def=daily_partition,
    group_name="datalake_core_meta"
)
def inventory_meta(context, config: MyConfig) -> Output[pl.DataFrame]:
    partition_date_str = context.asset_partition_key_for_output()
    print(f"Partition is {partition_date_str} for config {config.prop1}")
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
    partitions_def=daily_chunks_multi_part,
    ins={"inventory_meta": AssetIn(
        partition_mapping=daily_to_chunks,
    )},
    group_name="datalake_core_meta"
)
def split_inventory_meta(context, config: MyConfig, inventory_meta: pl.DataFrame) -> pl.DataFrame:
    partition_date_str = context.asset_partition_key_for_output()
    print(f"Split Multi Partition is {partition_date_str} for config {config.prop1}")
    chunked_df = inventory_meta.select(pl.all(),
                                  pl.col("Size").alias("CumSize").cast(pl.Int64).cumsum()
                                  ).with_columns(pl.all(),
                                                 pl.col("CumSize").alias("batch_num") // config.max_batch_size
                                                 ).partition_by("batch_num", as_dict=True)
    return chunked_df


split_inventory_meta_job = define_asset_job("split_inventory_meta_job", selection=AssetSelection.assets(split_inventory_meta))


@asset_sensor(asset_key=inventory_meta.key,
              job=split_inventory_meta_job,
              default_status=DefaultSensorStatus.RUNNING)
def split_inventory_sensor_meta(context: SensorEvaluationContext, asset_event: EventLogEntry):
    assert asset_event.dagster_event and asset_event.dagster_event.asset_key
    print(context.cursor)
    yield RunRequest(
        run_key=context.cursor,
        run_config=RunConfig(
            ops={
                "read_materialization": ReadMaterializationConfig(
                    asset_key=list(asset_event.dagster_event.asset_key.path)
                )
            }
        ),
    )


@asset(
    partitions_def=daily_chunks_multi_part,
    group_name="datalake_core_meta"
)
def coalesced_items_meta(context, config: MyConfig2, split_inventory_meta):
    """
    L
    """
    pass


@asset(
    partitions_def=daily_partition,
    ins={"coalesced_items_meta": AssetIn(
        partition_mapping=daily_to_chunks,
    )},
    group_name="datalake_core_meta"
)
def merged_items_meta(context, config: MyConfig2, coalesced_items_meta):
    """
    s
    """
    pass
