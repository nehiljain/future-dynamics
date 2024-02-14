import gspread
import pandas as pd
import pendulum
from dagster import AssetExecutionContext, Backoff, RetryPolicy, asset

from pipelines.partitions import daily_partitions

retry_policy = RetryPolicy(
    max_retries=3,
    delay=0.2,  # 200ms
    backoff=Backoff.EXPONENTIAL,
)


@asset(
    partitions_def=daily_partitions,
    io_manager_key="duckdb_io_manager",
    retry_policy=retry_policy,
    metadata={"partition_expr": "execution_at"},
)
def search_itineraries(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Pull in itineraries from google sheets
    """
    gc = gspread.service_account()
    sh = gc.open("B2Bed Hotel Pricing Data")
    worksheet = sh.get_worksheet(0)
    data = worksheet.get_all_records()

    def parse_date(x):
        try:
            return pendulum.from_format(x, "MMM D")
        except ValueError:
            return pendulum.from_format(x, "MMMM D")

    df = pd.DataFrame(data)
    df.rename(columns={"name": "hotel_name"}, inplace=True)
    # parse checkin using pendulum pendulum.from_format(df['checkin'], "MMM D")
    df["clean_checkin"] = df["checkin"].apply(lambda x: parse_date(x))
    df["clean_checkout"] = df["checkout"].apply(lambda x: parse_date(x))
    df["checkin_date"] = df["checkin"].apply(lambda x: parse_date(x).to_date_string())
    # df['run_at'] = pendulum.now('UTC').to_datetime_string()
    # df['execution_at'] = context.asset_partition_key_for_input("search_itineraries")
    df["length_of_stay"] = df.apply(
        lambda x: (x["clean_checkout"] - x["clean_checkin"]).days, axis=1
    )
    df["run_at"] = pendulum.now("UTC").to_datetime_string()
    df["execution_at"] = context.asset_partition_key_for_output()
    context.log.info("Data: df")
    return df


# def get_location(hotel_name):
#     params = {
#         "q": hotel_name,
#         "serp_api_key": EnvVar("SERPAPI_KEY").get_value(),
#         "limit": "1",
#     }

#     search = GoogleSearch(params)
#     results = search.get_dict()
#     return


# @asset(
#     partitions_def=daily_partitions,
#     # key_prefix=["gsheets"],
#     io_manager_key="duckdb_io_manager",
#     retry_policy=retry_policy,
#     metadata={"partition_expr": "run_at"},
# )
# def events(
#     context: AssetExecutionContext,
#     search_itineraries: pd.DataFrame,
# ) -> pd.DataFrame:
#     """
#     Gets all the events from predicthq for the locations of the hotels in search_itineraries
#     """
#     hotel_names = search_itineraries["hotel_name"].unique().tolist()
#     # get location for each hotel_name
#     hotel_locations = [get_location(hotel_name) for hotel_name in hotel_names]
