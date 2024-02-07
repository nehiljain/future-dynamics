
import re

import pendulum


def create_regex_patterns(hotel_name):
    """Create regex patterns based on the hotel name"""
    relevant_row_regex = re.compile(
        rf"{re.escape(hotel_name)}.*?\$\d+.*?Visit site", re.IGNORECASE
    )
    irrelevant_row_regex = re.compile(
        rf"^(?!.*{re.escape(hotel_name)}.*?\$\d+.*?Visit site).*", re.IGNORECASE
    )
    provider_price_row_regex = re.compile(
        rf"{re.escape(hotel_name)}.*?->(.*?);;\$(\d+)", re.IGNORECASE
    )
    return relevant_row_regex, irrelevant_row_regex, provider_price_row_regex

def model(dbt, session):
    df = dbt.source("gha", "hotel_prices").to_df()
    df['ai_input'] = df.apply(
        lambda x: f"{x['hotel_name']}__{x['checkin_date']}__{x['length_of_stay']}__{x['run_date']}->{x['text']}",
        axis=1,
    )
    relevant_row_regex, _, provider_price_row_regex = create_regex_patterns(df['hotel_name'].values[0])
    df["relevant"] = df["ai_input"].apply(lambda x: bool(relevant_row_regex.search(x)))
    df = df[df["relevant"]]
    df['list_price_usd'] = df['ai_input'].str.extract(provider_price_row_regex)[1].astype(float)
    df['list_price_provider'] = df['ai_input'].str.extract(provider_price_row_regex)[0]
    now = pendulum.now('UTC')
    df['run_date'] = now.to_date_string()
    # add a column for device type mobile or desktop based on the searching mobile or deskptop in the text
    df['device_type'] = df['text'].apply(lambda x: 'mobile' if 'mobile' in x else 'desktop')
    df['run_at'] = now.to_datetime_string()
    # drop ai_input, relevant col
    df = df.drop(columns=['ai_input', 'relevant'])
    return df
