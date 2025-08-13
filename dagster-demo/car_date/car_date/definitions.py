from dagster import Definitions, load_assets_from_modules

from . import assets  # noqa: TID252
from .jobs import car_price_job  # noqa: TID252
from .schedule import car_price_schedule  # noqa: TID252


all_assets = load_assets_from_modules([assets])
all_jobs = [car_price_job]
all_schedules = [car_price_schedule]

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
)
