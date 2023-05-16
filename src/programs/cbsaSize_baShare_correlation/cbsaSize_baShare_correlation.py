import numpy as np

from utils.read_and_preprocess_ipums_data import read_ipums_data, preprocess_ipums_data, weighted_mean



if __name__ == '__main__':
    ipums_df = read_ipums_data('../../ipums_data/usa_00013.xml')
    labforce_class_wkr = ipums_df.value_counts(['LABFORCE', 'CLASSWKR'], normalize=True)
    """       
    labforce 0<>N/A 1<>No, not in the labor force</labl> 2<>Yes, in the labor force
    classwkr 0<>N/A 1<>Self-employed 2<>Works for wages
    # How does the regressions change removing all labforce == 1
    """

    ipums_cbsa = preprocess_ipums_data(ipums_df)
    ipums_cbsa = ipums_cbsa[(ipums_cbsa['AGE'] >= 25) & (ipums_cbsa['AGE'] < 65)]

    size_baShare_corr = ipums_cbsa.groupby(['cbsa_code', 'cbsa']).agg({'ba': lambda x: weighted_mean(x),
                                                                       'PERWT': 'sum'}).reset_index()
    size_baShare_corr.to_csv('size_baShare_corr.csv', index=False)
