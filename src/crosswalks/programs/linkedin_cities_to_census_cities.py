import re

from config_files.snowflake_connection import export_data_from_sf
import pandas as pd

from crosswalks.constants import remove_words, us_state_to_abbrev

census = pd.read_csv('../files/numfipsa_csa.csv')
census = census[['csa_f_a', 'name_csa_f_a']].drop_duplicates().reset_index(drop=True)
census_split = census['name_csa_f_a'].str.rsplit(', ', n=1, expand=True).apply(lambda x: x.str.lower())
census_split.columns = ['csa', 'state']
census = pd.concat([census, census_split], axis=1)

cities_linkedin_query = """
select location_raw, count(location_raw) cnt
from DWH.PUBLIC.CONTACTS_DB
where iso = 'US'
group by 1
order by 2 desc
limit 200
;"""

cities_linkedin = export_data_from_sf(cities_linkedin_query)
for word in remove_words:
    cities_linkedin['LOCATION_RAW'] = cities_linkedin['LOCATION_RAW'].str.replace(word, '')
linkedin = cities_linkedin['LOCATION_RAW'].str.strip(' ,').str.split(', ', n=1, expand=True)
linkedin.columns = ['city', 'state']
linkedin['state_code'] = [us_state_to_abbrev.get(x.title()).lower() if isinstance(x, str) else None for x in linkedin['state']]


def _match_linkedin_city_to_csa(linkedin_city, linkedin_state, census_table):
    if linkedin_city == '':
        return None
    if linkedin_state:
        census_table = census_table[census_table['state'].str.contains(linkedin_state)].reset_index(drop=True)
    for i, r in census_table.iterrows():
        linkedin_city_tokens = linkedin_city.split()
        contained = 0
        csa = r['csa']
        for token in linkedin_city_tokens:
            if bool(re.search(f"\\b{token}\\b", csa)):
                contained += 1
        match_ratio = contained / len(linkedin_city_tokens)
        if match_ratio > 0.51:
            return csa
    return None


linkedin['csa'] = [_match_linkedin_city_to_csa(x, y, census) for x, y in zip(linkedin['city'], linkedin['state_code'])]
print('current_directory')