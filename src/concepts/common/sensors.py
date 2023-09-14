from typing import Optional
from dagster import (AssetsDefinition, asset_sensor, DefaultSensorStatus, SensorEvaluationContext, EventLogEntry,
                     DagsterEventType as Events, SkipReason, RunRequest, MultiPartitionKey, SensorResult,
                     multi_asset_sensor, MultiAssetSensorEvaluationContext,
                     AssetSelection, AssetKey, EventRecordsFilter, DagsterInstance, DynamicPartitionsDefinition)

from dagster._core.definitions.target import ExecutableDefinition


def build_asset_sensor_v1(sensor_name: str,
                          monitored_asset: AssetsDefinition,
                          target_job: ExecutableDefinition,
                          partition: DynamicPartitionsDefinition):
    @asset_sensor(
        asset_key=monitored_asset.key,
        name=sensor_name,
        default_status=DefaultSensorStatus.RUNNING,
        job=target_job)
    def _sensor(context: SensorEvaluationContext, event: EventLogEntry):
        upstream_asset_key = event.asset_materialization.asset_key
        upstream_partition_key = event.asset_materialization.partition
        print(f"Asset Keys detected by Sensor: {upstream_asset_key} and partition {upstream_partition_key}")

        if _should_trigger_run(context, event, upstream_asset_key, upstream_partition_key):
            meta = event.asset_materialization.metadata
            chunks_mdv = meta["chunks"]
            all_chunks = chunks_mdv.value
            print(
                f"Sensor Found Materialization for Partition Key = {upstream_asset_key} \n"
                f"and Chunk Count = {len(all_chunks)}")
            context.update_cursor(cursor=upstream_partition_key)
            parti_request = [partition.build_add_request([a_chunk]) for a_chunk in all_chunks if (
                not partition.has_partition_key(partition_key=a_chunk,
                                                               dynamic_partitions_store=context.instance))]
            all_run_requests = [RunRequest(partition_key=MultiPartitionKey(
                keys_by_dimension={"date": upstream_partition_key, "chunks": downstream_partition})) for
                downstream_partition in all_chunks]
            print(f"Partitions creation Total = {len(parti_request)} \n"
                  f"and created total run requests {len(all_run_requests)}")
            return SensorResult(
                run_requests=all_run_requests,
                dynamic_partitions_requests=parti_request,
            )
        else:
            no_mat_msg = f"No Materialization For Key: {upstream_asset_key}"
            print(no_mat_msg)
            return SkipReason(skip_message=no_mat_msg)

    def _should_trigger_run(context, event, upstream_asset_key, upstream_partition_key):
        event_type_is_materialization:bool = event.dagster_event_type == Events.ASSET_MATERIALIZATION
        partition_is_materialized:bool = _is_partition_materialized(
            upstream_asset_key,
            upstream_partition_key,
            context.instance)
        print(f"Checking if Event Type is Materialization: {event_type_is_materialization}\n"
              f"Checking if Partition is Materialized: {partition_is_materialized}")
        return event_type_is_materialization and partition_is_materialized

    def _is_partition_materialized(asset_key: AssetKey, partition_key: str, instance: DagsterInstance) -> bool:
        return bool(
            instance.get_event_records(
                EventRecordsFilter(
                    asset_key=asset_key,
                    event_type=Events.ASSET_MATERIALIZATION,
                    asset_partitions=[partition_key],
                ),
                limit=1,
            )
        )

    return _sensor







def build_asset_sensor_v2(sensor_name: str,
                          monitored_asset: AssetsDefinition,
                          target_job: ExecutableDefinition,
                          downstream_asset: Optional[AssetsDefinition]):
    @multi_asset_sensor(sensor_name=sensor_name,
                        monitored_asset=[monitored_asset.key],
                        job=target_job,
                        request_assets=AssetSelection.assets(downstream_asset))
    def _sensor(context: MultiAssetSensorEvaluationContext):
        print(f"Sensed it !! {context.asset_keys}")
        asset_key, event = context.latest_materialization_records_by_key(monitored_asset.key)

    return _sensor



