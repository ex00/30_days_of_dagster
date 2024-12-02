from dagster import schedule, SkipReason, RunRequest,AssetSelection,ScheduleDefinition
from .assets_day2 import c
import datetime

@schedule(
    cron_schedule="* * * * *",
    target=AssetSelection.groups("advent_asserts") | AssetSelection.assets(c.node_def.name),
    execution_timezone="UTC"
)
def day2_schedule_definition():
    now = datetime.datetime.now()
    if now.minute % 2 == 0:
        return RunRequest()
    return SkipReason(f"Not now, it's just {now}")

day2_schedule_definition_second = ScheduleDefinition(
    name="day2_schedule_definition_second",
    target=AssetSelection.assets(c.node_def.name).upstream(),
    cron_schedule="* * * * *",  
)