import dagster as dg


car_price_job = dg.define_asset_job(
    name="car_price_job",
    selection=dg.AssetSelection.all()
)