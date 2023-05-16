import pandas as pd
from functools import reduce

from utils.read_and_preprocess_ipums_data import read_ipums_data, preprocess_ipums_data
from utils.snowflake_connection import export_data_from_sf, embed_list_in_query


def _compute_entrep_shares_by_city(df, var):
    df_group = df.groupby('cbsa').agg({var: ['count', 'sum']}).reset_index()
    df_group.columns = ['cbsa', f'count_{var}', f'sum_{var}']
    df_group[f'share_{var}'] = df_group[f'sum_{var}'] / df_group[f'count_{var}']
    return df_group


linkedin_sample_query = """select *
from (
select LOCATION_RAW id, 
case when (contains(function, 'Owner') or contains(function, 'Founder') or job_title = 'general director') then 1 else 0 end entrep
from DWH.PUBLIC.CONTACTS_DB
where {0}
and is_current = TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY START_TIME DESC NULLS LAST) = 1) entrep_table
sample({1})
;"""


ipums_df = read_ipums_data('../../ipums_data/usa_00013.xml')
ipums_cbsa = preprocess_ipums_data(ipums_df)

# Load LinkedIn data sample
linkedin_cbsa_cw = pd.read_csv('../../crosswalks/files/cbsa/linkedin_city_cbsa.csv')
linkedin_sample = export_data_from_sf(linkedin_sample_query,
                                      (embed_list_in_query(linkedin_cbsa_cw['id'], 'LOCATION_RAW'), 5))
linkedin_sample.columns = linkedin_sample.columns.str.lower()
linkedin_sample = linkedin_sample.merge(linkedin_cbsa_cw, on='id', how='left')

shares = []
for table, entrep_var in zip([ipums_cbsa, ipums_cbsa, linkedin_sample, ipums_cbsa], ['SELF', 'INC', 'entrep', 'ba']):
    shares.append(_compute_entrep_shares_by_city(table, entrep_var))

shares_comparison = reduce(lambda left, right: pd.merge(left, right, on=['cbsa'], how='outer'), shares)

shares_comparison.to_csv('entrep_share_by_cbsa.csv', index=False)
z = 1
