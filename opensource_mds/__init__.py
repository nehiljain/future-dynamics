from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
)

from . import assets, resources, constants
from dagster_duckdb_pandas import DuckDBPandasIOManager

all_assets = load_assets_from_modules([assets])
duckdb_resource = resources.duckdb_resource
assets_job = define_asset_job(
    name="google_hotel_scraper",
    selection=all_assets,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 4,
                },
            }
        }
    },
)

hotel_prices_job = define_asset_job("google_hotel_prices", selection=[assets.hotel_prices])

# search_itineraries_job = define_asset_job("fetch_search_itineraries", selection=[assets.search_itineraries])
# search_itineraries_schedule = build_schedule_from_partitioned_job(search_itineraries_job)

hotel_prices_schedule = build_schedule_from_partitioned_job(
  hotel_prices_job,
)



defs = Definitions(
  assets=all_assets,
  resources={
      "duckdb_io_manager": DuckDBPandasIOManager(database=constants.DUCK_DB_FILE),  # required
  },
  schedules=[hotel_prices_schedule]
)
