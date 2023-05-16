import pandas as pd

from utils.correlation_plots import two_variable_scatter_plot_with_adjusted_regression_line
from utils.snowflake_connection import export_data_from_sf

funding_by_city_query = """
SELECT HEADQUARTERS_LOCATION, LAST_FUNDING_TYPE, SUM(LAST_FUNDING_AMOUNT_USD)
FROM "COMPANIES_DATA_ENRICHMENT"."PUBLIC"."FUNDING_CURRENT" FUNDING
WHERE HEADQUARTERS_LOCATION LIKE '%United States%'
GROUP BY 1, 2
;
"""

funding_by_city = export_data_from_sf(funding_by_city_query)
crunchbase_cw = pd.read_csv('../../crosswalks/crunchbase_to_census_cities/crunchbase_cbsa.csv')
baShare_city = pd.read_csv('../cbsaSize_baShare_correlation/size_baShare_corr.csv')

funding = funding_by_city.merge(crunchbase_cw, left_on='HEADQUARTERS_LOCATION', right_on='id')
funding['SUM(LAST_FUNDING_AMOUNT_USD)'].fillna(0, inplace=True)
funding_types = ['Grant', 'Angel', 'Venture - Series Unknown', 'Pre-Seed', 'Series A', 'Series B', 'Series C',
                 'Series D', 'Series E', 'Series F', 'Series G', 'Series H']
funding = funding[funding['LAST_FUNDING_TYPE'].isin(funding_types)]
funding = funding.groupby(['cbsa', 'cbsa_code'])[['SUM(LAST_FUNDING_AMOUNT_USD)']].sum().reset_index()

funding = funding.merge(baShare_city, on=['cbsa', 'cbsa_code'], how='outer')

funding_corr = funding.dropna()
funding_corr = funding_corr[funding_corr['SUM(LAST_FUNDING_AMOUNT_USD)'] > 0]
funding_corr['funding_per_capita'] = funding_corr['SUM(LAST_FUNDING_AMOUNT_USD)'] / funding_corr['PERWT']

funding_corr = funding_corr.sort_values(by=['PERWT'], ascending=False).reset_index(drop=True)
down = 0
for top in [10, 50, 100, 208]:
    x = funding_corr.loc[down:top, 'ba']
    y = funding_corr.loc[down:top, 'funding_per_capita']
    two_variable_scatter_plot_with_adjusted_regression_line(x, y, f'fundingPerCapita-baShare. Top {down}-{top}')
    down = top

two_variable_scatter_plot_with_adjusted_regression_line(funding_corr['ba'], funding_corr['funding_per_capita'],
                                                        f'fundingPerCapita-baShare')

1
