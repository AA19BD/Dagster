import dagster as dg
from .jobs import car_price_job


car_price_schedule = dg.ScheduleDefinition(
    job=car_price_job,
    cron_schedule="* * * * *",  # Every minute for testing purposes
    execution_timezone="UTC",
    description="Daily job to process car price data and update DuckDB table.",
)   