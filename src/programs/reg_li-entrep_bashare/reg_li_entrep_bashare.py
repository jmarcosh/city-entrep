import pandas as pd

from programs.self_control_lhs.reg_entrep_bashare_self import reghdfe_and_save_results_csv, \
    weighted_ols_and_save_results
from utils.read_and_preprocess_ipums_data import read_ipums_data, preprocess_ipums_data
from utils.snowflake_connection import export_data_from_sf, embed_list_in_query

linkedin_sample_query = """select *
from (
select contact_id, LOCATION_RAW id, 
case when ((lower(job_title) = 'owner' or lower(job_title) = 'co-owner') and EMPLOYEES_IN_LINKEDIN < 500) 
or contains(function, 'Founder') then 1 else 0 end entrep,
EMPLOYEES_IN_LINKEDIN, COMPANIES.company_id, industry
from DWH.PUBLIC.CONTACTS_DB PERSONS
left join DWH.PUBLIC.COMPANIES_DB COMPANIES on COMPANIES.COMPANY_ID = PERSONS.COMPANY_ID
where {0}
and is_current = TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY START_TIME DESC NULLS LAST) = 1) entrep_table
sample({1})
;"""


if __name__ == '__main__':

    ipums_df = read_ipums_data('../../ipums_data/usa_00016.xml')
    ipums_df = ipums_df[(ipums_df['AGE'] >= 25) & (ipums_df['AGE'] < 65)]
    ipums_cbsa = preprocess_ipums_data(ipums_df)
    # ipums_cbsa = pd.read_csv('../../ipums_data/usa_00016_sample.csv')

    # Load LinkedIn data sample
    linkedin_cbsa_cw = pd.read_csv('../../crosswalks/files/cbsa/linkedin_city_cbsa.csv')
    linkedin_sample = export_data_from_sf(linkedin_sample_query,
                                          (embed_list_in_query(linkedin_cbsa_cw['id'], 'LOCATION_RAW'), 5))
    linkedin_sample.columns = linkedin_sample.columns.str.lower()
    linkedin_sample['company_match'] = 0
    linkedin_sample.loc[~linkedin_sample['company_id'].isna(), 'company_match'] = 1
    linkedin_sample.loc[(linkedin_sample['company_id'].str.contains('stealth')) &
                        (linkedin_sample['company_id'].str.contains('startup')), 'employees_in_linkedin'] = 2
    linkedin_sample['employees_in_linkedin'] = linkedin_sample['employees_in_linkedin'].fillna(linkedin_sample['company_match']).fillna(0).astype('int')
    linkedin_sample['industry'] = linkedin_sample['industry'].fillna('Unknown')
    linkedin_sample = linkedin_sample.merge(linkedin_cbsa_cw, on='id', how='left')
    linkedin_census = linkedin_sample.merge(ipums_cbsa[['cbsa_code', 'baShare', 'selfShare']].drop_duplicates(),
                                            on=['cbsa_code'], how='left').dropna(subset='baShare')
    linkedin_census['industry_code'] = linkedin_census['industry'].astype('category').cat.codes
    linkedin_census['PERWT'] = 1
    predictors = ['baShare', 'selfShare']
    category_input = ['industry_code']

    for col in predictors:
        linkedin_census[col] = linkedin_census[col].astype(float)

    for threshold in ['', 1, 5, 10, 20, 50, 100]:
        if threshold != '':
            linkedin_census['aux'] = linkedin_census['li_entrep'] * linkedin_census['employees_in_linkedin']
            linkedin_census['aux'] = linkedin_census[f'aux'].fillna(0).astype(int)
            linkedin_census[f'li_entrep{threshold}'] = 0
            linkedin_census.loc[linkedin_census['aux'] >= threshold, f'li_entrep{threshold}'] = 1
            print('Percentage of entrepreneurs', threshold, linkedin_census[f'li_entrep{threshold}'].sum()/len(linkedin_census))
        else:
            linkedin_census['li_entrep'] = linkedin_census['entrep']
            print('Percentage of entrepreneurs', threshold, linkedin_census['li_entrep'].sum() / len(linkedin_census))

        reghdfe_and_save_results_csv(linkedin_census, predictors, [f'li_entrep{threshold}'], category_input, f'li_entrep{threshold}_bashare')
        weighted_ols_and_save_results(linkedin_census, predictors, f'li_entrep{threshold}', 'PERWT', f'li_entrep{threshold}_bashare')
1