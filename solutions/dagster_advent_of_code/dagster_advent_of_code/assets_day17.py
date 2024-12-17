from dagster import asset, multi_asset, AssetExecutionContext, AssetIn, Output, AssetOut
import random

@asset
def a():
    return random.choices([random.randint(11, 20), random.randint(0, 10)], [0.3, 0.7])[0]

@multi_asset(
        # specs=[AssetSpec("b", deps=["a"]), AssetSpec("c", deps=["a"])],
        outs={
            "c": AssetOut(is_required=False, dagster_type=int),
            "b": AssetOut(is_required=False, dagster_type=int)
        },
        ins={"a_val": AssetIn(key=["a"])}
        )
def my_complex_assets(context: AssetExecutionContext, a_val): 
    context.log.info(f"Value of a is {a_val}")

    if a_val > 10:
        yield Output(a_val, output_name='b')
    else:
        yield Output(a_val, output_name='c')
