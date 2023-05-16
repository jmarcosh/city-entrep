import FixedEffectModelPyHDFE.api as fem
import pandas as pd
import statsmodels.api as sm

from utils.read_and_preprocess_ipums_data import read_ipums_data, preprocess_ipums_data


def generate_summary_chart(result):
    params = result.params.reset_index()
    params['estimator'] = 'beta'
    pvalues = result.pvalues.reset_index()
    pvalues['estimator'] = 'pvalue'
    table = pd.concat([params, pvalues]).reset_index()
    table = table.sort_values(by=['level_0', 'estimator']).drop(columns='level_0')
    # general_table = pd.DataFrame(output.general_table)
    # general_table1 = general_table.loc[:5, [0, 1]]
    # general_table1.columns = ['index', 0]
    # general_table2 = general_table[[2, 3]]
    # general_table2.columns = ['index', 0]
    return table #pd.concat([table, general_table1, general_table2])


def reghdfe_and_save_results_csv(df, independent_vars, dependent_var, categorical_vars, suffix):
    result = fem.ols_high_d_category(df, independent_vars, dependent_var, categorical_vars)
    summary = generate_summary_chart(result)
    summary.to_csv(f'reghdfe_{suffix}.csv', index=False)


def weighted_ols_and_save_results(df, independent_vars, dependent_var, weights, suffix):
    X = df[independent_vars]
    X = sm.add_constant(X)
    y = df[dependent_var]
    model = sm.OLS(y, X)
    weights = df[weights]
    result_ols = model.fit(weights=weights)
    with open(f'ols_{suffix}.csv', 'w') as f:
        f.write(result_ols.summary().as_csv())


if __name__ == '__main__':

    ipums_df = read_ipums_data('../../ipums_data/usa_00016.xml')
    ipums_df = ipums_df[(ipums_df['AGE'] >= 25) & (ipums_df['AGE'] < 65)]
    ipums_cbsa = preprocess_ipums_data(ipums_df)

    # ipums_cbsa = pd.read_csv('/home/jmarcosh/Projects/city-entrep/src/ipums_data/usa_00016_sample.csv')

    name_lst = ['self', 'no_self']
    predictors_lst = [['baShare', 'selfShare', 'hs', 'somecol', 'col', 'grad', 'FEMALE', 'ASIAN', 'BLACK', 'HISPAN', 'MARRIED', 'AGE', 'AGE2'],
                      ['baShare', 'hs', 'somecol', 'col', 'grad', 'FEMALE', 'ASIAN', 'BLACK', 'HISPAN', 'MARRIED', 'AGE', 'AGE2']]
    fixed_effects = ['OCC', 'IND']
    output_variable = ['INC']

    for predictors, name in zip(predictors_lst, name_lst):
        for col in predictors:
            ipums_cbsa[col] = ipums_cbsa[col].astype(float)
        reghdfe_and_save_results_csv(predictors, output_variable, fixed_effects, f'enrep_bashare_{name}')
        weighted_ols_and_save_results(predictors, 'INC', 'PERWT', f'enrep_bashare_{name}')
