from dagster import asset, AssetExecutionContext

@asset 
def a(): 
    print("Hello dagster")

@asset(
    deps=[a]
) 
def b(context: AssetExecutionContext):
    context.log.info("Hello")

@asset(
    deps=[b]
) 
def c(context: AssetExecutionContext): 
    context.log.info("Hello")