from typing import List

import polars as pl

from dagster import (asset,
                     Config,
                     AssetIn,
                     define_asset_job as build_job_from_assets,
                     AssetSelection,
                     StepExecutionContext
                     )

from concepts.complex_partitioning import fake_data
from concepts.complex_partitioning import helper
from concepts.complex_partitioning.partitions import daily_partition, daily_chunks_multi_part, daily_to_chunks


class ReadMaterializationConfig(Config):
    asset_key: List[str]


class MyConfig(Config):
    state_id: str = "Hello"
    full_refresh: bool = False
    max_batch_size: int = 150 * 1024 * 1024
    record_count: int = 5000


@asset(
    partitions_def=daily_partition,
    group_name="datalake_core"
)
def inventory(context: StepExecutionContext, config: MyConfig) -> pl.DataFrame:
    partition_id = context.asset_partition_key_for_output()
    print(f"Partition Output Key is {partition_id} for config {config.max_batch_size}")
    inventory_df = fake_data.generate_fake_dataframe()
    chunked_dfs = helper.make_dataframe_chunks(config, inventory_df)
    all_chunks_str = [f"chunk-{x}" for x in chunked_dfs.keys()]
    helper.save_all_chunks(chunked_dfs)
    meta_data = {"row_count": inventory_df.height,
                 "chunk_count": len(chunked_dfs.keys()),
                 "chunks": all_chunks_str}
    context.add_output_metadata(metadata=meta_data)
    return inventory_df


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



