from dagster import asset, AssetExecutionContext, AssetIn, ScheduleDefinition, AssetSelection
import random

@asset
def a():
    return random.choices([random.randint(11, 20), random.randint(0, 10)], [0.3, 0.7])[0]

@asset(
    ins={"val": AssetIn(key=["a"])}
)
def b(context: AssetExecutionContext, val):
    return context.add_output_metadata({'input': val})

schedule = ScheduleDefinition(
    name="assets_schedule",
    target=[a, b],
    cron_schedule="* * * * *", 
)