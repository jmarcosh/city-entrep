from utils.snowflake_connection import export_data_from_sf
from crosswalks.linkedin_to_census_cities.functions import create_crosswalk_object
from crosswalks.linkedin_to_census_cities.queries import cities_linkedin_query




if __name__ == '__main__':
    cities_linkedin = export_data_from_sf(cities_linkedin_query)

    # Process LinkedIn extraction
    cities_linkedin['LOCATION_RAW_CLEAN'] = cities_linkedin['LOCATION_RAW']
    for sub in [', united states', ', estados unidos', ', verenigde staten', ', amerika serikat',
                ', vereinigte staaten von amerika', ', us']:
        cities_linkedin['LOCATION_RAW_CLEAN'] = cities_linkedin['LOCATION_RAW_CLEAN'].str.replace(sub, '')
        linkedin = cities_linkedin['LOCATION_RAW_CLEAN'].str.split(', ', n=1, expand=True)
    cw = create_crosswalk_object()
    cw.create_crosswalk_from_city_and_state_df(linkedin, '../files/cbsa/linkedin_city_cbsa')

    # test = cw.match_linkedin_city_to_cbsa('madison', 'wi')
