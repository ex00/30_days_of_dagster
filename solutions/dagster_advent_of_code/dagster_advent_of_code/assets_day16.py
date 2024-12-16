from dagster import asset, multi_asset, AssetExecutionContext ,AssetSpec, AssetIn, Output, AssetOut


@asset(
    ins={"a_vals": AssetIn(key=["c"])}
)
def a(context: AssetExecutionContext, a_vals):
    return context.add_output_metadata({'input': a_vals}) 

@asset(
    ins={"b_vals": AssetIn(key=["d"])}
)
def b(context: AssetExecutionContext, b_vals):
    return context.add_output_metadata({'input': b_vals})


@multi_asset(
        # specs=[AssetSpec("c", skippable=True), AssetSpec("d", skippable=True)],
        outs={
            "c": AssetOut(is_required=False, dagster_type=str),
            "d": AssetOut(is_required=False, dagster_type=list)
        },
        can_subset=True,
        )
def my_complex_assets(context: AssetExecutionContext): 
    
    if "c" in context.selected_output_names:
        data = list(range(10,20,5))
        context.log.info(f"Data: {data}")
        yield Output(str(data),output_name='c')
    
    if "d" in context.selected_output_names:
        data = list(range(20,45,5))
        context.log.info(f"Data: {data}")
        yield Output(data, output_name='d')