import numpy as np
from ipumspy import IpumsApiClient, UsaExtract, readers
import pandas as pd

from utils.load_config_files import load_config
import os


def read_ipums_data(file_path):
    ddi = readers.read_ipums_ddi(file_path)
    location = file_path.rsplit('/', 1)[0] + '/'
    ipums_df = readers.read_microdata(ddi, location + ddi.file_description.filename)
    return ipums_df


def weighted_mean(series, df, weights):
    return np.average(series, weights=df.loc[series.index, weights])


def _merge_state_county_fip(state, county):
    return int(str(state) + str(county).zfill(3))


def generate_education_variables(df, var, lower, upper):
    df[var] = 0
    df.loc[(df['EDUCD'] >= lower) & (df['EDUCD'] <= upper), var] = 1
    return df


def add_inc_and_self_indicators_to_ipums_df(df):
    df['INC'] = 0
    df.loc[(df['CLASSWKRD'] == 14), 'INC'] = 1
    df['SELF'] = 0
    df.loc[(df['CLASSWKRD'] == 14) | (df['CLASSWKRD'] == 13), 'SELF'] = 1


def add_education_indicators_to_ipums_df(df):
    for i in [('hs', 61, 64), ('somecol', 65, 100), ('col', 101, 113), ('grad', 114, 116)]:
        df = generate_education_variables(df, *i)
    df['ba'] = df['col'] + df['grad']


def convert_county_fips_to_cbsa(df):
    fips = [_merge_state_county_fip(x, y) for x, y in zip(df['STATEFIP'], df['COUNTYFIP'])]
    county_cbsa_cw = pd.read_csv(os.path.join(os.path.dirname(__file__), "../crosswalks/files/cbsa/county_cbsa.csv"))
    """
    The following codes were changed manually in the crosswalk
    39150: Prescott Valley-Prescott, AZ -> 39140: Prescott, AZ
    19430: Dayton-Kettering, OH -> 19380: Dayton, OH
    """
    fips_cbsa_dict = county_cbsa_cw.set_index('fips')['cbsa_code'].to_dict()
    df['cbsa_code'] = [fips_cbsa_dict.get(fips) if cbsa == 0 else cbsa
                       for fips, cbsa in zip(fips, df['MET2013'])]
    df_cbsa = df.merge(county_cbsa_cw[['cbsa_code', 'cbsa']].drop_duplicates(), on='cbsa_code', how='left')
    return df_cbsa.dropna(subset=['cbsa_code'])


def add_aggregated_vars_to_ipums_df(df):
    educ_entrep_ratios = df.groupby(['cbsa_code', 'cbsa']).agg({'ba': lambda x: weighted_mean(x, df, 'PERWT'),
                                                                        'SELF': lambda x: weighted_mean(x, df, 'PERWT')})
    educ_entrep_ratios.rename({'ba': 'baShare', 'SELF': 'selfShare'}, axis=1, inplace=True)
    return df.merge(educ_entrep_ratios, on=['cbsa_code', 'cbsa'], how='left')


def add_demographic_indicators_to_ipums_df(df):
    df["FEMALE"] = df["SEX"] - 1
    df["ASIAN"] = 0
    df.loc[(df["RACE"] > 3) & (df["RACE"] < 7), "ASIAN"] = 1
    df["BLACK"] = 0
    df.loc[(df["RACE"] == 2), "BLACK"] = 1
    df.loc[(df["HISPAN"] > 0), "HISPAN"] = 1
    df["MARRIED"] = 0
    df.loc[(df["MARST"] == 0) | (df["MARST"] == 1), "MARRIED"] = 1
    df["AGE2"] = df["AGE"] ** 2


def preprocess_ipums_data(df):
    df = df[df['CLASSWKRD'] > 0].reset_index(drop=True)
    df = convert_county_fips_to_cbsa(df)
    add_inc_and_self_indicators_to_ipums_df(df)
    add_education_indicators_to_ipums_df(df)
    df = add_aggregated_vars_to_ipums_df(df)
    add_demographic_indicators_to_ipums_df(df)
    return df


# # Get the DDI
# ddi_file = list(DOWNLOAD_DIR.glob("*.xml"))[0]
# ddi = readers.read_ipums_ddi(ddi_file)
#
# # Get the data
# ipums_df = readers.read_microdata(ddi, DOWNLOAD_DIR / ddi.file_description.filename)
#
# # specify the path to the .dat.gz file
# file_path = "usa_00010.dat.gz"
#
# # read the file using gzip and numpy
# with gzip.open(file_path, 'rb') as f:
#     data = np.frombuffer(f.read(), dtype=np.float32)
