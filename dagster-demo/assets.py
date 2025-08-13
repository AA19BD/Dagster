import dagster as dg


@dg.asset
def hello(context: dg.AssetExecutionContext) -> None:
    context.log.info("Hello, Dagster!")

@dg.asset(deps=[hello])
def world(context: dg.AssetExecutionContext) -> None:
    context.log.info("Hello, World!")

df = dg.Definitions(assets=[hello, world])