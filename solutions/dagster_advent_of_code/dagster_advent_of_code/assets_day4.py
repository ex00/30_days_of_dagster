from dagster import asset, AutomationCondition, RetryPolicy ,Backoff, Failure 
import random

@asset(
    retry_policy=RetryPolicy(max_retries=4, delay=0.1, backoff = Backoff.EXPONENTIAL),
    automation_condition=AutomationCondition.any_downstream_conditions()
) 
def a(): 
    if random.randint(1,2) > 1:
        raise Failure("No luck :( ")
    return

@asset(
    deps=[a],
    automation_condition=AutomationCondition.any_downstream_conditions()
) 
def b(): ... 

@asset(
    deps=[b],
    automation_condition=AutomationCondition.on_cron("* * * * *")
) 
def c(): ...