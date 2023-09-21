import os
from typing import List, Sequence, Optional
from datetime import datetime, timedelta
from dagster import (
    AssetsDefinition,
    DynamicPartitionsDefinition,
    asset_sensor,
    DefaultSensorStatus,
    SensorEvaluationContext,
    EventLogEntry,
    RunRequest,
    MultiPartitionKey,
    SensorResult,
    SkipReason,
    AssetKey,
    EventLogRecord,
    EventRecordsFilter,
    DagsterEventType as Events,
    multi_asset_sensor,
    AssetSelection,
    MultiAssetSensorEvaluationContext,
    JobDefinition,
    MultiPartitionsDefinition,
    SensorDefinition,
    DagsterEventType,
    PartitionsDefinition,
)
from dagster._core.definitions.target import ExecutableDefinition

from .partition_finder import PartitionFinder

DEFAULT_SENSOR_INTERVAL: int = int(os.getenv("SENSOR_MINIMUM_INTERVAL_SECONDS", 30))


def build_sensor_for_partitioned_assets(
        sensor_name: str,
        upstream_asset: AssetsDefinition,
        downstream_job: List[ExecutableDefinition],
        partition: PartitionsDefinition,
        partitions_finder: PartitionFinder,
        description: str = None,
):
    """
    Triggers one or more jobs when the upstream_asset is materialized. It also adds
    Args:
        partitions_finder:
        sensor_name:
        upstream_asset:
        downstream_job:
        partition:
        description:

    Returns: Sensor object

    """
    job_names = ",".join([ajob.name for ajob in downstream_job])
    default_description = f"Monitors Asset {upstream_asset.key.to_string()} and triggers jobs [{job_names}] using {partition}"

    @asset_sensor(
        asset_key=upstream_asset.key,
        name=sensor_name,
        default_status=DefaultSensorStatus.RUNNING,
        jobs=downstream_job,
        minimum_interval_seconds=int(DEFAULT_SENSOR_INTERVAL),
        description=description if description else default_description,
    )
    def _sensor(context: SensorEvaluationContext, event: EventLogEntry):
        all_run_requests: List[RunRequest] = []
        backfill_time = timedelta(seconds=(DEFAULT_SENSOR_INTERVAL * 4))
        back_dated_ts = float((datetime.now() - backfill_time).strftime("%s"))

        asset_keys = [event.asset_materialization.asset_key]
        context.log.info(
            f"Sensor Triggered for Asset Key -> {event.asset_materialization.asset_key}"
        )
        materialized_asset_records = _find_assets_materialized_after_timestamp(
            context=context,
            upstream_asset_key=asset_keys[0],
            last_sensor_tick=back_dated_ts,
        )

        for a_record in materialized_asset_records:
            all_chunks = partitions_finder.find_partitions(
                a_record.asset_materialization
            )
            upstream_partition_key = a_record.partition_key
            asset_name = a_record.asset_key.to_user_string()
            log_msg_detection = f"Found Materialization Record : {asset_name}|{upstream_partition_key} ## {all_chunks}"

            context.log.info(log_msg_detection)
            _add_partition_if_dynamic(all_chunks, context, partition)

            for ajob in downstream_job:
                all_run_requests += [
                    _new_run_request(run_key=a_record.run_id,
                                     upstream_partition_key=upstream_partition_key,
                                     downstream_partition=downstream_partition,
                                     downstream_job_name=ajob.name)
                    for downstream_partition in all_chunks
                ]

        return (
            SensorResult(run_requests=all_run_requests)
            if len(all_run_requests) > 0
            else SkipReason("No evaluated Run Request")
        )

    def _new_run_request(run_key, upstream_partition_key, downstream_partition, downstream_job_name):
        return RunRequest(
            run_key=run_key,
            partition_key=MultiPartitionKey(
                keys_by_dimension={
                    "date": upstream_partition_key,
                    "parts": downstream_partition,
                }
            ),
            job_name=downstream_job_name,
        )

    def _add_partition_if_dynamic(all_chunks, context, the_partition):
        dyn_part: DynamicPartitionsDefinition = None
        if isinstance(partition, DynamicPartitionsDefinition):
            dyn_part = the_partition
        elif isinstance(partition, MultiPartitionsDefinition):
            partition.__class__ = MultiPartitionsDefinition
            for dim in partition.partition_dimension_names:
                sub_dyn_part = partition.get_partitions_def_for_dimension(dim)
                if isinstance(sub_dyn_part, DynamicPartitionsDefinition):
                    dyn_part = sub_dyn_part

        if dyn_part:
            context.instance.add_dynamic_partitions(
                partitions_def_name=dyn_part.name, partition_keys=all_chunks
            )

    return _sensor


def build_chunked_asset_aggregating_sensor(
        sensor_name: str,
        upstream_asset: AssetsDefinition,
        downstream_jobs: List[ExecutableDefinition],
        partition: PartitionsDefinition,
        partitions_finder: PartitionFinder,
        description: str = None,
):
    """
    Triggers one or more jobs when the upstream_asset is materialized. It also adds
    Args:
        partitions_finder:
        sensor_name:
        upstream_asset:
        downstream_jobs:
        partition:
        description:

    Returns: Sensor object

    """
    job_names = ",".join([ajob.name for ajob in downstream_jobs])
    default_description = f"Monitors Asset {upstream_asset.key.to_string()} and triggers jobs [{job_names}] using {partition}"

    @asset_sensor(
        asset_key=upstream_asset.key,
        name=sensor_name,
        default_status=DefaultSensorStatus.RUNNING,
        jobs=downstream_jobs,
        minimum_interval_seconds=int(DEFAULT_SENSOR_INTERVAL),
        description=description if description else default_description,
    )
    def _sensor(context: SensorEvaluationContext, event: EventLogEntry):
        all_run_requests: List[RunRequest] = []
        backfill_time = timedelta(seconds=(DEFAULT_SENSOR_INTERVAL * 4))
        back_dated_ts = float((datetime.now() - backfill_time).strftime("%s"))

        asset_keys = [event.asset_materialization.asset_key]
        context.log.info(
            f"Sensor Triggered for Asset Key -> {event.asset_materialization.asset_key}"
        )
        materialized_asset_records = _find_assets_materialized_after_timestamp(
            context=context,
            upstream_asset_key=asset_keys[0],
            last_sensor_tick=back_dated_ts,
        )

        for a_record in materialized_asset_records:
            all_chunks = partitions_finder.find_partitions(
                a_record.asset_materialization
            )
            upstream_partition_key = a_record.partition_key
            asset_name = a_record.asset_key.to_user_string()
            log_msg_detection = f"Found Materialization Record : {asset_name}|{upstream_partition_key} ## {all_chunks}"

            context.log.info(log_msg_detection)
            _add_partition_if_dynamic(all_chunks, context, partition)

            for ajob in downstream_jobs:
                all_run_requests += [
                    _new_run_request(run_key=a_record.run_id,
                                     upstream_partition_key=upstream_partition_key,
                                     downstream_partition=downstream_partition,
                                     downstream_job_name=ajob.name)
                    for downstream_partition in all_chunks
                ]

        return (
            SensorResult(run_requests=all_run_requests)
            if len(all_run_requests) > 0
            else SkipReason("No evaluated Run Request")
        )

    def _new_run_request(run_key, upstream_partition_key, downstream_partition, downstream_job_name):
        return RunRequest(
            run_key=run_key,
            partition_key=MultiPartitionKey(
                keys_by_dimension={
                    "date": upstream_partition_key,
                    "parts": downstream_partition,
                }
            ),
            job_name=downstream_job_name,
        )

    def _add_partition_if_dynamic(all_chunks, context, the_partition):
        dyn_part: DynamicPartitionsDefinition = None
        if isinstance(partition, DynamicPartitionsDefinition):
            dyn_part = the_partition
        elif isinstance(partition, MultiPartitionsDefinition):
            partition.__class__ = MultiPartitionsDefinition
            for dim in partition.partition_dimension_names:
                sub_dyn_part = partition.get_partitions_def_for_dimension(dim)
                if isinstance(sub_dyn_part, DynamicPartitionsDefinition):
                    dyn_part = sub_dyn_part

        if dyn_part:
            context.instance.add_dynamic_partitions(
                partitions_def_name=dyn_part.name, partition_keys=all_chunks
            )

    return _sensor


def _find_assets_materialized_after_timestamp(context: SensorEvaluationContext,
                                              upstream_asset_key: AssetKey,
                                              last_sensor_tick: float) -> Sequence[EventLogRecord]:
    run_records = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=Events.ASSET_MATERIALIZATION,
            asset_key=upstream_asset_key,
            after_timestamp=last_sensor_tick,
        ),
        ascending=False,
    )

    return run_records







def build_multi_asset_sensor(
        sensor_name: str,
        monitored_assets: Sequence[AssetKey],
        downstream_job: List[ExecutableDefinition],
        downstream_asset: Sequence[AssetKey],
):
    job_names = ",".join([ajob.name for ajob in downstream_job])
    monitored_asset_names = ",".join([an_asset.to_user_string() for an_asset in monitored_assets])
    default_description = f"Monitors Assets {monitored_asset_names} and triggers jobs [{job_names}] using {partition}"

    @multi_asset_sensor(
        sensor_name=sensor_name,
        monitored_asset=monitored_assets,
        jobs=downstream_job,
        request_assets=AssetSelection.keys(*downstream_asset),
        description=default_description
    )
    def _sensor(context: MultiAssetSensorEvaluationContext):
        print(f"Sensed materializations for Assets {context.asset_keys}")
        asset_key, event = context.latest_materialization_records_by_key()
        # TODO TBD
        yield SkipReason("Still not implemented !!, Needs to be implemented!")
        context.update_cursor_after_evaluation()

    return _sensor
