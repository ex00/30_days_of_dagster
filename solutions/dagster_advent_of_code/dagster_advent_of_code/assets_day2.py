from dagster import asset, AssetExecutionContext, schedule, SkipReason, RunRequest,AssetSelection ,HourlyPartitionsDefinition

@asset(
    group_name="advent_asserts",
    tags={"advent_code_assert": "a"},
)
def a(): 
    print("Hello dagster")

@asset(
    deps=[a],
    group_name="advent_asserts",
    tags={"advent_code_assert": "b"},
) 
def b(context: AssetExecutionContext):
    context.log.info("Hello")

@asset(
    name="last_assert",
    deps=[b],
    tags={"advent_code_assert": "c"},
) 
def c(context: AssetExecutionContext): 
    context.log.info("Hello")

