import sys
import os

from config_files.snowflake_connection import export_data_from_sf

cities_linkedin_query = """
select location_raw, count(location_raw) cnt
from DWH.PUBLIC.CONTACTS_DB
where iso = 'US'
group by 1
order by 2 desc
limit 200
;"""

cities_linkedin = export_data_from_sf(cities_linkedin_query)
city = cities_linkedin['LOCATION_RAW'].str.rsplit(',', n=1, expand=True)
print('current_directory')
