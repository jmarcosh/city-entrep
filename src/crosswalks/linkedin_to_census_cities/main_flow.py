from utils.snowflake_connection import export_data_from_sf
from crosswalks.linkedin_to_census_cities.functions import create_crosswalk_object
from crosswalks.linkedin_to_census_cities.queries import cities_linkedin_query

cw = create_crosswalk_object()

test = cw.match_linkedin_city_to_cbsa('madison', 'wi')
cities_linkedin = export_data_from_sf(cities_linkedin_query)

# Process LinkedIn extraction
cities_linkedin['LOCATION_RAW_CLEAN'] = cities_linkedin['LOCATION_RAW']
for sub in [', united states', ', estados unidos', ', verenigde staten', ', amerika serikat',
            ', vereinigte staaten von amerika', ', us']:
    cities_linkedin['LOCATION_RAW_CLEAN'] = cities_linkedin['LOCATION_RAW_CLEAN'].str.replace(sub, '')
    linkedin = cities_linkedin['LOCATION_RAW_CLEAN'].str.split(', ', n=1, expand=True)
linkedin.columns = ['city', 'state']
linkedin['state_code'] = cw.get_state_code(linkedin['state'])

linkedin['cbsa'] = [cw.match_linkedin_city_to_cbsa(x, y) for x, y in zip(linkedin['city'], linkedin['state_code'])]
linkedin['id'] = cities_linkedin['LOCATION_RAW']
linkedin['cnt'] = cities_linkedin['CNT']
linkedin_crosswalk = (linkedin.merge(cw.county_map.drop_duplicates(subset=['cbsa']),
                                     on='cbsa', how='left')[['id', 'cbsa', 'cbsa_code']])
linkedin_crosswalk.to_csv('../files/linkedin_city_cbsa.csv', index=False)
linkedin.to_csv('../files/linkedin_city_cbsa_analysis.csv', index=False)

