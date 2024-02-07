from datetime import datetime

import pendulum
from dagster import DailyPartitionsDefinition, TimeWindowPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(start_date=datetime(2024, 2, 1))

four_times_daily_partition = TimeWindowPartitionsDefinition(
    start=pendulum.datetime(2024, 1, 21, 0, 0),
    cron_schedule="6 */6 * * *",
    fmt="%Y-%m-%d-%H-%M",
)
