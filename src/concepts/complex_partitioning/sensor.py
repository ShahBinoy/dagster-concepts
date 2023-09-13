from dagster import AssetsDefinition, asset_sensor, DefaultSensorStatus, SensorEvaluationContext, EventLogEntry, \
    DagsterEventType, SkipReason, RunRequest, MultiPartitionKey, SensorResult

from concepts.complex_partitioning.assets import coalesce_items_job, dynamic_chunks_partition


def build_sensor_using_asset_sensor(sensor_name: str, monitored_asset: AssetsDefinition):
    @asset_sensor(
        asset_key=monitored_asset.key,
        name=sensor_name,
        default_status=DefaultSensorStatus.RUNNING,
        job=coalesce_items_job)
    def split_inventory_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
        upstream_asset_key = asset_event.asset_materialization.asset_key
        upstream_partition_key = asset_event.asset_materialization.partition
        print(f"Asset Keys detected by Sensor: {upstream_asset_key} and partition {upstream_partition_key}")
        if asset_event.dagster_event_type != DagsterEventType.ASSET_MATERIALIZATION:
            no_mat_msg = f"No Materialization For Key: {upstream_asset_key}"
            print(no_mat_msg)
            return SkipReason(skip_message=no_mat_msg)
        else:
            medadata = asset_event.asset_materialization.metadata
            chunks_mdv = medadata["chunks"]
            all_chunks = chunks_mdv.value
            print(
                f"Sensor Found Materialization for Partition Key = {upstream_asset_key} \n"
                f"and Chunk Count = {len(all_chunks)}")
            context.update_cursor(cursor=upstream_partition_key)
            parti_request = [dynamic_chunks_partition.build_add_request([a_chunk]) for a_chunk in all_chunks if (
                not dynamic_chunks_partition.has_partition_key(partition_key=a_chunk,
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

    return split_inventory_sensor
