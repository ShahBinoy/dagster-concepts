from ..common.sensors import build_asset_sensor_v1
from .assets import inventory
from .jobs import coalesce_inventory_job
from .partitions import dynamic_chunks_partition

split_inventory_sensor = build_asset_sensor_v1(sensor_name="inventory_sensor",
                                               monitored_asset=inventory,
                                               target_job=coalesce_inventory_job,
                                               partition=dynamic_chunks_partition)
