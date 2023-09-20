from ..common.asset_sensor_builder import build_sensor_for_partitioned_assets
from .assets import inventory
from .jobs import coalesce_inventory_job
from .partitions import dynamic_chunks_partition
from ..common.partition_finder import AssetMetaPartitionFinder
split_inventory_sensor = build_sensor_for_partitioned_assets(sensor_name="inventory_sensor",
                                                             upstream_asset=inventory,
                                                             downstream_job=[coalesce_inventory_job],
                                                             partition=dynamic_chunks_partition,
                                                             partitions_finder=AssetMetaPartitionFinder(meta_field_name="parts"))
