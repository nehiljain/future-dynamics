from dagster import (
    AssetSelection,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from .assets import EVENTS, GOOGLE_HOTEL_ADS

events_job = define_asset_job(
    name="EL_events_job",
    selection=AssetSelection.groups(EVENTS),
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

gha_job = define_asset_job("gha_job", selection=AssetSelection.groups(GOOGLE_HOTEL_ADS))


events_schedule = build_schedule_from_partitioned_job(
    events_job,
)

gha_schedule = build_schedule_from_partitioned_job(
    gha_job,
)
