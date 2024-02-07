import os

from dagster import Definitions, EnvVar
from dagster_duckdb_pandas import DuckDBPandasIOManager

from .assets import events_assets, google_hotel_ads_assets
from .jobs import events_job, events_schedule, gha_job, gha_schedule
from .resources import dbt_resource

all_assets = [*events_assets, *google_hotel_ads_assets]

# resources_by_deployment_name = {
#     # "prod": RESOURCES_PROD,
#     # "staging": RESOURCES_STAGING,
#     "local": RESOURCES_LOCAL,
# }

deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

all_jobs = [events_job, gha_job]

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb_io_manager": DuckDBPandasIOManager(
            database=EnvVar("DUCK_DB_FILE")
        ),  # required
        "dbt": dbt_resource,
    },
    schedules=[gha_schedule, events_schedule],
)