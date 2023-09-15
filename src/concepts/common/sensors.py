from typing import Optional, Sequence, List
from dagster import (AssetsDefinition, asset_sensor, DefaultSensorStatus, SensorEvaluationContext, EventLogEntry,
                     DagsterEventType as Events, SkipReason, RunRequest, MultiPartitionKey, SensorResult,
                     multi_asset_sensor, MultiAssetSensorEvaluationContext,
                     AssetSelection, AssetKey, EventRecordsFilter, DagsterInstance, DynamicPartitionsDefinition,
                     RunsFilter, DagsterRunStatus, EventLogRecord)

from dagster._core.definitions.target import ExecutableDefinition

sensor_tick_interval: int = 30


def build_asset_sensor_for_downstream_job(sensor_name: str,
                                          monitored_asset: AssetsDefinition,
                                          target_job: ExecutableDefinition,
                                          partition: DynamicPartitionsDefinition):
    @asset_sensor(
        asset_key=monitored_asset.key,
        name=sensor_name,
        default_status=DefaultSensorStatus.RUNNING,
        job=target_job,
        minimum_interval_seconds=int(sensor_tick_interval / 2))
    def _sensor(context: SensorEvaluationContext, event: EventLogEntry):
        from datetime import datetime, timedelta
        back_dated_ts = float(
            (datetime.now() - timedelta(seconds=(sensor_tick_interval * 2))).strftime('%s'))

        asset_keys = [event.asset_materialization.asset_key]
        print(f"Sensor Triggered for Asset Key -> {event.asset_materialization.asset_key}")
        materialized_asset_records = _asset_materializations_since_last_tick(context=context,
                                                                             upstream_asset_key=asset_keys[0],
                                                                             last_sensor_tick=back_dated_ts)
        skipped_asset_partitions: List[str] = []
        all_run_requests: List[RunRequest] = []

        for asset_materialization in materialized_asset_records:
            chunks_mdv = asset_materialization.asset_materialization.metadata.get('chunks')
            all_chunks = chunks_mdv.value if chunks_mdv else []
            upstream_partition_key = asset_materialization.partition_key
            asset_name = asset_materialization.asset_key.to_user_string()
            no_mat_msg = (f"Asset|Partition: {asset_name}|{upstream_partition_key}\n"
                          f"    Chunks: {all_chunks}")
            print(no_mat_msg)
            context.instance.add_dynamic_partitions(partitions_def_name=partition.name,
                                                    partition_keys=all_chunks)
            all_run_requests += [
                RunRequest(run_key=upstream_partition_key,
                           partition_key=MultiPartitionKey(
                               keys_by_dimension={"date": upstream_partition_key,
                                                  "chunks": downstream_partition})) for
                downstream_partition in all_chunks]

        return SensorResult(
            run_requests=all_run_requests
        )

    def _asset_materializations_since_last_tick(context: SensorEvaluationContext,
                                                upstream_asset_key: AssetKey,
                                                last_sensor_tick: float,
                                                force_run: bool = False) -> Sequence[EventLogRecord]:
        run_records = context.instance.get_event_records(
            EventRecordsFilter(
                event_type=Events.ASSET_MATERIALIZATION,
                asset_key=upstream_asset_key,
                after_timestamp=last_sensor_tick
            ),
            ascending=False
        )

        return run_records

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
