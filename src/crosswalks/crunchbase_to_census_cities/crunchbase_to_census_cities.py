import pandas as pd

from crosswalks.linkedin_to_census_cities.functions import create_crosswalk_object
from utils.snowflake_connection import export_data_from_sf


cities_crunchbase_query = """
SELECT HEADQUARTERS_LOCATION, COUNT(HEADQUARTERS_LOCATION) CNT
FROM "COMPANIES_DATA_ENRICHMENT"."PUBLIC"."FUNDING_CURRENT"
WHERE HEADQUARTERS_LOCATION LIKE '%United States%'
GROUP BY 1
"""

cities_crunchbase = export_data_from_sf(cities_crunchbase_query)
crunchbase = cities_crunchbase['HEADQUARTERS_LOCATION'].str.split(', ', n=2, expand=True)
crunchbase.drop([2], axis=1, inplace=True)
crunchbase.columns = ['city', 'state']
crunchbase['id'] = cities_crunchbase['HEADQUARTERS_LOCATION']
crunchbase['cnt'] = cities_crunchbase['CNT']  # FIX DO IN ONE DF
cw = create_crosswalk_object()
cw.create_crosswalk_from_city_and_state_df(crunchbase, 'crunchbase_cbsa')

# test = pd.DataFrame({'city': ['Foster City'], 'state': ['California'], 'id': [1], 'cnt': [1]})
