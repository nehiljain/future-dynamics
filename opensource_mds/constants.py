CHECKLIST_2020="https://clo-pfw-prod.s3.us-west-2.amazonaws.com/data/202306/PFW_all_2016_2020_June2023_Public.zip"
CHECKLIST_2023="https://clo-pfw-prod.s3.us-west-2.amazonaws.com/data/202306/PFW_all_2021_2023_June2023_Public.zip"
SITE_DESCRIPTION_DATA="https://clo-pfw-prod.s3.us-west-2.amazonaws.com/data/202306/PFW_count_site_data_public_June2023.zip"
SPECIES_TRANSLATION_DATA="https://clo-pfw-prod.s3.us-west-2.amazonaws.com/data/202306/PFW_spp_translation_table_May2023.zip"

CL_2020_FPATH="../data/raw/checklist_data/PFW_all_2016_2020_June2023_Public.csv"
CL_2023_FPATH="../data/raw/checklist_data/PFW_all_2021_2023_June2023_Public.csv"
SITE_DATA_FPATH="../data/raw/checklist_data/PFW_count_site_data_public_June2023.csv"
SPECIES_TRANSLATION_FPATH="../data/raw/checklist_data/PFW_spp_translation_table_May2023.csv"

DUCK_DB_FILE ="/Users/nehiljain/code/data/future-dynamics/raw/hotel_pricer_ai.duckdb"
SCREENSHOTS_DPATH ="/Users/nehiljain/code/data/future-dynamics/raw/"

STEAMPIPE_QUERY="""
with data as (
        select
        regexp_matches(content, '(#[^#\\s]+)', 'g') as hashtag
        from mastodon_toot_local limit 1000
)

select
    hashtag,
    count(1) as count
from data
group by 1
order by count desc, hashtag;
"""
