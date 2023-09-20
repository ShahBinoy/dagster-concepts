from datetime import date, timedelta

from dagster import DailyPartitionsDefinition, DynamicPartitionsDefinition, MultiPartitionsDefinition, \
    MultiToSingleDimensionPartitionMapping

start_dt = (date.today() - timedelta(days=9)).strftime('%Y-%m-%d')
daily_partition = DailyPartitionsDefinition(start_date=start_dt)
dynamic_chunks_partition = DynamicPartitionsDefinition(name="parts")
daily_chunks_multi_part = MultiPartitionsDefinition(
    partitions_defs={"date": daily_partition, "parts": dynamic_chunks_partition})
daily_to_chunks = MultiToSingleDimensionPartitionMapping(partition_dimension_name="date")
