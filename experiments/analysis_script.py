import re
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import pendulum
import seaborn as sns

DUCK_DB_FILE ="/Users/nehiljain/code/data/future-dynamics/raw/hotel_pricer_ai.duckdb"

with duckdb.connect(DUCK_DB_FILE, read_only=True) as con:
  hotel_prices = con.execute("SELECT * FROM public.hotel_prices ").df()
  print(hotel_prices)



# SQL: Get search itineraries from db
search_itineraries_sql = """
SELECT *
from public.search_itineraries
"""

hotel_prices_trend_sql = """
SELECT *
FROM public.hotel_prices
where text not ilike '%fail%'
and hotel_name = '{hotel_name}'
"""

def get_price_comparison_graph(search_itinerary, plot_directory):
    with duckdb.connect(DUCK_DB_FILE, read_only=True) as con:
      hotel_prices_trend = hotel_prices_trend_sql.format(hotel_name=search_itinerary['hotel_name'])
      hotel_prices_trend_df = con.execute(hotel_prices_trend).df()
      if hotel_prices_trend_df.empty:
        return
      hotel_prices_trend_df = hotel_prices_trend_df.assign(
        list_price_usd=hotel_prices_trend_df['text']
        .apply(lambda x: re.findall(r'\$\d+', x)[0][1:] if re.findall(r'\$\d+', x) else None)
        .pipe(pd.to_numeric, errors='coerce')
      )
      # modify run_at to be a pendulum format YYYY-mm-dd-HH
      hotel_prices_trend_df['run_at'] = hotel_prices_trend_df['run_at'].apply(lambda x: pendulum.parse(x).format("YYYY-MM-DD-HH"))
      # find the min list_price_usd by hotel_name and run_at in hotel_prices_trend_df
      min_hotel_prices_trend_df = hotel_prices_trend_df.groupby('run_at').agg({'list_price_usd': 'min'}).reset_index().rename(columns={'list_price_usd': 'min_list_price_usd'})
      max_hotel_prices_trend_df = hotel_prices_trend_df.groupby('run_at').agg({'list_price_usd': 'max'}).reset_index().rename(columns={'list_price_usd': 'max_list_price_usd'})

      hotel_prices_trendlines_df = pd.merge(max_hotel_prices_trend_df, min_hotel_prices_trend_df, on=['run_at'])

      search_itinerary['list_price_usd'] = float(search_itinerary['list_price_usd'].replace('$', ''))
      #series repeat list_price_usd for length of date spine
      buy_price_usd = pd.Series(search_itinerary['list_price_usd'], index=hotel_prices_trendlines_df['run_at'])
      # create a df from series list_price_usd and date_spine
      buy_price_usd = pd.DataFrame(buy_price_usd, columns=['buy_price_usd'])

      # merge buy price usd to hotel_prices_trend_df on run_at.
      # This will create a df with run_at, buy_price_usd, min_list_price_usd, max_list_price_usd
      final_df = pd.merge(hotel_prices_trendlines_df, buy_price_usd, on='run_at')
      # sns plot for a straight line over time on list_price_usd x axis is dates
      plt.figure(figsize=(10,12))
      sns.set(rc={'figure.figsize':(11.7,8.27)})
      plt.title(f'{search_itinerary["hotel_name"]}--{search_itinerary["checkin_date"]}--{search_itinerary["length_of_stay"]}')
      sns.lineplot(data=final_df, x="run_at", y="buy_price_usd")
      sns.lineplot(data=final_df, x="run_at", y="min_list_price_usd")
      sns.lineplot(data=final_df, x="run_at", y="max_list_price_usd")
      plt.xticks(rotation=90)
      sanitized_hotel_name = re.sub(r'[^\w\s]', '', search_itinerary['hotel_name'])
      datetime_str = pendulum.now('UTC').format("YYYY-MM-DD")
      plt_filename = Path(plot_directory, datetime_str, f'{sanitized_hotel_name}_{search_itinerary["checkin_date"]}_{search_itinerary["length_of_stay"]}_{pendulum.now("UTC").format("YYYYMMDD")}.png')
      plt_filename.parent.mkdir(parents=True, exist_ok=True)
      plt.savefig(plt_filename, format='png')
      print(f"plot stored at {plt_filename}")
    #   plt.show()
      return final_df

print(f'This is search itineraries {search_itineraries_sql}')
with duckdb.connect(DUCK_DB_FILE, read_only=True) as con:
  hotel_prices = con.execute("SELECT * FROM public.hotel_prices ").df()
  search_itineraries = con.execute('select * from public.search_itineraries').df().to_dict(orient='records')
  for search_itinerary in search_itineraries:
      search_itinerary['clean_created_at'] = pendulum.from_format(search_itinerary['created_at'] + '-2024', 'MMM D-YYYY')
      search_itinerary['url'] = None
      print(search_itinerary)
      df = get_price_comparison_graph(search_itinerary, '/Users/nehiljain/code/data/future-dynamics/reporting/plots/')


# with duckdb.connect(DUCK_DB_FILE, read_only=True) as con:
#   search_itineraries = con.execute(search_itineraries_sql).df().to_dict(orient='records')
#   for search_itinerary in search_itineraries:
#       print(search_itinerary)
#       df = get_price_comparison_graph(search_itinerary, '/Users/nehiljain/code/data/future-dynamics/reporting/plots/')


