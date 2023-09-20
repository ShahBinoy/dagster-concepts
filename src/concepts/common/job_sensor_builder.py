import os
from typing import List

from dagster import (
    JobDefinition,
    SensorDefinition,
    DefaultSensorStatus,
    sensor,
    SensorEvaluationContext,
    RunsFilter,
    DagsterRunStatus,
    RunRequest,
    SensorResult,
    MultiPartitionsDefinition,
    MultiPartitionKey, SkipReason,
)


DEFAULT_SENSOR_INTERVAL: int = int(os.getenv("SENSOR_MINIMUM_INTERVAL_SECONDS", 30))


def build_job_sensor(
    sensor_name: str,
    upstream_job: JobDefinition,
    downstream_job: List[JobDefinition],
    sensor_interval: int = DEFAULT_SENSOR_INTERVAL,
    description: str = None,
    start_disabled: bool = True,
) -> SensorDefinition:
    """Factory method to create a Sensor that is watching a certain job and
    triggering the target job when the sensor thresholds are met

            This function will attach a sensor to runs based on any external state change
            of a job.

            Args:
                sensor_name: Name of the sensor.
                upstream_job: The job which, the sensor monitors for completion.
                downstream_job: Job which will be triggered by sensor.
                sensor_interval: defaults to 30 seconds
                start_disabled: If True, the initial state of sensor is going to be STOPPED,
                                else it will be RUNNING, Default False
                description: sensor description

            Returns:
                Dagster Sensor Definition
    """
    sensor_state = (
        DefaultSensorStatus.STOPPED if start_disabled else DefaultSensorStatus.RUNNING
    )

    @sensor(
        name=sensor_name,
        jobs=downstream_job,
        minimum_interval_seconds=sensor_interval,
        default_status=sensor_state,
        description=description,
    )
    def _job_sensor(context: SensorEvaluationContext):
        """Execute job .

        This function will execute runs based on any external state change
        of a job.

        Args:
            name: Name of the sensor.
            job: Job which will be triggered by sensor.

        Returns:
            Dagster run request object
        """
        run_records = context.instance.get_run_records(
            filters=RunsFilter(
                job_name=upstream_job.name,
                statuses=[DagsterRunStatus.SUCCESS],
            ),
            ascending=False,
            limit=1,
        )

        for run_record in run_records:
            yield RunRequest(
                run_key=run_record.dagster_run.run_id,  # avoid double firing for the same run
            )

    return _job_sensor


def build_partitioned_job_sensor(
    sensor_name: str,
    upstream_job: JobDefinition,
    downstream_job: List[JobDefinition],
    partition_def: MultiPartitionsDefinition,
    description: str = None,
) -> SensorDefinition:
    """Bind's a sensor based on job name.

    This function will attach a sensor to runs based on any external state change
    of a job.

    Args:
        upstream_job: Job name on which sensor depends on.
        sensor_name: Name of the sensor.
        downstream_job: Job which will be triggered by sensor.
        partition_def:  partition definition
        description: sensor description

    Returns:
        Dagster Sensor Definition
    """

    @sensor(name=sensor_name, jobs=downstream_job, description=description)
    def run_job_sensor(context: SensorEvaluationContext):
        """Execute job .

        This function will execute runs based on any external state change
        of a job.

        Args:
            context:
            name: Name of the sensor.
            job: Job which will be triggered by sensor.

        Returns:
            Dagster run request object
        """
        run_records = context.instance.get_run_records(
            filters=RunsFilter(
                job_name=upstream_job.name,
                statuses=[DagsterRunStatus.SUCCESS],
            ),
            ascending=False,
        )
        mult_part = []
        run_check = dict()

        run_requests = []

        for run_record in run_records:
            run_data = run_record.dagster_run.tags["dagster/partition"].split("|")
            if run_check.get(run_data[1]) is None:
                mult_part.append({"chunks": run_data[0], "date": run_data[1]})
                run_check[run_data[1]] = [run_data[0]]
            else:
                if run_data[0] not in run_check[run_data[1]]:
                    mult_part.append({"chunks": run_data[0], "date": run_data[1]})
                    run_check[run_data[1]] = [run_data[0]]

            run_requests.append(
                RunRequest(
                    tags=partition_def.get_tags_for_partition_key(
                        MultiPartitionKey({"chunks": run_data[0], "date": run_data[1]})
                    ),
                    run_key=run_record.dagster_run.run_id,
                )
            )
        del run_check

        return SensorResult(run_requests=run_requests)

    return run_job_sensor


# def sensor_between_partitioned_jobs(
#     sensor_name: str,
#     upstream_job: JobDefinition,
#     downstream_job: List[JobDefinition],
#     description: str = None,
# ) -> SensorDefinition:
#     """Bind's a sensor based on job name which have partitioned assets.
#
#     This function will attach a sensor to runs based on any external state change
#     of a job.
#
#     Args:
#         upstream_job: Job name on which sensor depends on.
#         sensor_name: Name of the sensor.
#         downstream_job: Job which will be triggered by sensor.
#         description: sensor description
#
#     Returns:
#         Dagster Sensor Definition
#     """
#
#     @sensor(name=sensor_name, jobs=downstream_job, description=description)
#     def run_job_sensor(context):
#         run_records = context.instance.get_run_partition_data(
#             runs_filter=RunsFilter(
#                 job_name=upstream_job.name,
#                 statuses=[DagsterRunStatus.SUCCESS],
#             )
#         )
#
#         for run_record in run_records:
#             partition = run_record.partition
#             index = run_record.partition.find("|")
#             if index and index > 0:
#                 partition_list = run_record.partition.split("|")
#                 partition = partition_list[1]
#             return SensorResult(
#                 run_requests=[
#                     RunRequest(
#                         partition_key=partition,
#                         run_key=run_record.run_id,
#                         job_name=jb.name,
#                     )
#                     for jb in downstream_job
#                 ]
#             )
#
#     return run_job_sensor
def sensor_with_two_job_dependency(
    sensor_name: str,
    upstream_job: JobDefinition,
    parent_job_upstream_job: JobDefinition,
    downstream_job: List[JobDefinition],
    description: str = None,
) -> SensorDefinition:
    """Bind's a sensor based on job name which have partitioned assets.

    This function will attach a sensor to runs based on any external state change
    of a job.

    Args:
        upstream_job: Job name on which sensor depends on.
        parent_job_upstream_job:  Job name of parent of upstream job
        sensor_name: Name of the sensor.
        downstream_job: Job which will be triggered by sensor.
        description: sensor description

    Returns:
        Dagster Sensor Definition
    """

    @sensor(
        name=sensor_name,
        jobs=downstream_job,
        minimum_interval_seconds=120,
        description=description,
    )
    def run_job_sensor(context):
        run_records = context.instance.get_run_records(
            filters=RunsFilter(
                job_name=parent_job_upstream_job.name,
                statuses=[
                    DagsterRunStatus.QUEUED,
                    DagsterRunStatus.NOT_STARTED,
                    DagsterRunStatus.STARTING,
                    DagsterRunStatus.STARTED,
                ],
            ),
            limit=1,
            ascending=False,
        )
        if len(run_records) <= 0:
            run_records = context.instance.get_run_records(
                filters=RunsFilter(
                    job_name=upstream_job.name,
                    statuses=[
                        DagsterRunStatus.QUEUED,
                        DagsterRunStatus.NOT_STARTED,
                        DagsterRunStatus.STARTING,
                        DagsterRunStatus.STARTED,
                    ],
                ),
                limit=1,
                ascending=False,
            )
            if len(run_records) <= 0:
                run_requests_list = []
                run_records = context.instance.get_run_records(
                    filters=RunsFilter(
                        job_name=upstream_job.name,
                        statuses=[DagsterRunStatus.SUCCESS],
                    ),
                    limit=1,
                    ascending=False,
                )
                for run_record in run_records:
                    partition = run_record.dagster_run.tags["dagster/partition"]
                    index = run_record.dagster_run.tags["dagster/partition"].find("|")
                    if index:
                        partition_list = run_record.dagster_run.tags[
                            "dagster/partition"
                        ].split("|")
                        partition = partition_list[1]
                    run_requests_list.append(
                        RunRequest(
                            partition_key=partition,
                            run_key=run_record.dagster_run.run_id,
                        )
                    )
                if len(run_requests_list) > 0:
                    return SensorResult(run_requests=run_requests_list)
                else:
                    SkipReason(f"No new run for wear job")
            else:
                SkipReason(f"No new run for wear job since steps job still running")
        else:
            SkipReason(f"No new run for wear job since coalesced ")

    return run_job_sensor
