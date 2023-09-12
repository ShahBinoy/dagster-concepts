import os
from typing import List
from datetime import datetime
from faker import Faker

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
                     define_asset_job as build_job_from_assets,
                     AssetSelection,
                     DefaultSensorStatus,
                     SkipReason,
                     StepExecutionContext,
                     SensorResult, DagsterEventType,
                     MultiPartitionKey
                     )

from concepts.complex_partitioning import utils

Faker.seed(4321)

start_date = datetime.strptime(os.getenv("DATALAKE_START_DATE", '2023-09-01'), '%Y-%m-%d')

daily_partition = DailyPartitionsDefinition(start_date=start_date)

dynamic_chunks_partition = DynamicPartitionsDefinition(name="chunks")

daily_chunks_multi_part = MultiPartitionsDefinition(
    partitions_defs={"date": daily_partition, "chunks": dynamic_chunks_partition})

daily_to_chunks = MultiToSingleDimensionPartitionMapping(partition_dimension_name="date")


class ReadMaterializationConfig(Config):
    asset_key: List[str]


class MyConfig(Config):
    state_id: str = "Hello"
    full_refresh: bool = False
    max_batch_size: int = 10 * 1024 * 1024
    record_count: int = 5000


@asset(
    partitions_def=daily_partition,
    group_name="datalake_core"
)
def inventory(context: StepExecutionContext, config: MyConfig) -> pl.DataFrame:
    partition_id = context.asset_partition_key_for_output()
    # part_range = context.asset_partition_key_range_for_output()
    # part_time_window = context.asset_partitions_time_window_for_output()
    print(f"Partition Output Key is {partition_id} for config {config.max_batch_size}")
    inventory_df = utils.generate_fake_dataframe()
    chunked_dfs = inventory_df.select(pl.col("*"),
                                      pl.col("Size").alias("CumSize").cast(pl.Int64).cumsum()
                                      ).with_columns(pl.col("*"),
                                                     pl.col("CumSize").alias("batch_num") // config.max_batch_size
                                                     ).partition_by("batch_num", as_dict=True)
    all_chunks_str = [f"chunk-{x}" for x in chunked_dfs.keys()]
    for k, v in chunked_dfs.items():
        save_df(k, v)
    context.add_output_metadata(
        metadata={"row_count": inventory_df.height, "chunk_count": len(chunked_dfs.keys()), "chunks": all_chunks_str})
    return inventory_df


def save_df(k, v):
    if k == 0:
        print(f"Saving Data Frame ... \n {v}")


@asset(
    partitions_def=daily_chunks_multi_part,
    group_name="datalake_core",
    ins={"inventory": AssetIn(
        partition_mapping=daily_to_chunks,
    )}
)
def coalesce_items(context, config: MyConfig, inventory):
    partition_date_str = context.asset_partition_key_for_output()
    print(f"Split Multi Partition is {partition_date_str} for config {config.max_batch_size}")


@asset(
    partitions_def=daily_partition,
    group_name="datalake_core"
)
def items_for_tenant(context, config: MyConfig, coalesce_items):
    """
    This is the documentation of an Asset, Also visible on UI
    """
    pass


coalesce_items_job = build_job_from_assets("coalesce_items_job", selection=AssetSelection.assets(coalesce_items),
                                           partitions_def=daily_chunks_multi_part)


@asset_sensor(
    asset_key=inventory.key,
    name="inventory_sensor",
    default_status=DefaultSensorStatus.RUNNING,
    job=coalesce_items_job)
def split_inventory_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    upstream_asset_key = asset_event.asset_materialization.asset_key
    upstream_partition_key = asset_event.asset_materialization.partition
    print(f"Asset Keys detected by Sensor: {upstream_asset_key} and partition {upstream_partition_key}")
    if asset_event.dagster_event_type != DagsterEventType.ASSET_MATERIALIZATION:
        no_mat_msg = f"No Materialization For Key: {upstream_asset_key}"
        print(no_mat_msg)
        yield SkipReason(skip_message=no_mat_msg)
    else:
        medadata = asset_event.asset_materialization.metadata
        print(f"Medadata type == {type(medadata)}")
        chunks_mdv = medadata["chunks"]
        print(f"Chunk MDV type == {type(chunks_mdv)}")
        chunks__ = chunks_mdv.value
        print(f"Found Chunks {chunks__}")
        print(
            f"Sensor Found Materialization for Partition Key = {upstream_asset_key} \n"
            f"and Chunk Count = {len(chunks__)}")
        parti_request = [dynamic_chunks_partition.build_add_request([a_chunk]) for a_chunk in chunks__]
        all_run_requests = [RunRequest(partition_key=MultiPartitionKey(
            keys_by_dimension={"date": upstream_partition_key, "chunks": downstream_partition})) for downstream_partition in chunks__]
        print(f"Partitions creation Total = {len(parti_request)} \n"
              f"and created total run requests {len(all_run_requests)}")
        yield SensorResult(
            run_requests=all_run_requests,
            dynamic_partitions_requests=parti_request,
        )
