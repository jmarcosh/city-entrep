from typing import List
import re

import pandas as pd
from uszipcode import SearchEngine
from crosswalks.linkedin_to_census_cities.constants import us_state_to_abbrev, remove_words


class LinkedinCrosswalk:
    def __init__(self):
        self.search = SearchEngine()
        # self.census_table = None
        self.gpt_map = None
        self.county_map = None

    def load_files(self):
        # self.read_and_process_census_file()
        self.read_and_process_gpt_file()
        self.read_and_process_county_cbsa_crosswalk()

    # def read_and_process_census_file(self):
    #     census = pd.read_csv('../files/numfipsa_cbsa.csv')
    #     # Process census file
    #     census = census[['cbsa_f_a', 'name_cbsa_f_a']].drop_duplicates().reset_index(drop=True)
    #     census_split = census['name_cbsa_f_a'].str.rsplit(', ', n=1, expand=True).apply(lambda x: x.str.lower())
    #     census_split.columns = ['cbsa', 'state']
    #     census = pd.concat([census, census_split], axis=1)
    #     self.census_table = census

    def read_and_process_gpt_file(self):
        gpt_map = pd.read_csv('../files/cbsa/gpt_linkedin_city_cbsa.csv')
        gpt_map['state_code'] = self.get_state_code(gpt_map['state'])
        self.gpt_map = gpt_map

    def read_and_process_county_cbsa_crosswalk(self):
        county_cbsa = pd.read_csv('../files/cbsa/county_cbsa.csv')
        county_cbsa = county_cbsa.drop_duplicates(subset=['county', 'state_name']).reset_index(drop=True)
        county_cbsa['state_code'] = self.get_state_code(county_cbsa['state_name'])
        county_cbsa['state'] = county_cbsa['cbsa'].str.rsplit(', ').str[1]
        self.county_map = county_cbsa

    @staticmethod
    def get_state_code(state_lst: List) -> List:
        state_code = []
        for x in list(state_lst):
            if isinstance(x, str):
                x = x.replace(' metropolitan area', '').replace(' area', '')
                try:
                    state_code.append(us_state_to_abbrev.get(x.title()))
                except AttributeError:
                    state_code.append(None)
                    print(x)
            else:
                state_code.append(None)
        return state_code

    def match_linkedin_city_to_cbsa(self, linkedin_city, linkedin_state):
        linkedin_city_tokens = re.split(r'\W', linkedin_city.lower())
        linkedin_city_tokens = [token for token in linkedin_city_tokens if token not in remove_words]
        if len(linkedin_city_tokens) == 0:
            return None
        census_cbsa = self.county_map.drop_duplicates(subset='cbsa')
        if linkedin_state:
            census_cbsa = census_cbsa[census_cbsa['state'].str.contains(linkedin_state)].reset_index(drop=True)
        for i, r in census_cbsa.iterrows():
            contained = 0
            cbsa_lookup = r['cbsa'].split(', ')[0].lower()
            for token in linkedin_city_tokens:
                if bool(re.search(f"\\b{token}\\b", cbsa_lookup)):
                    contained += 1
            match_ratio = contained / len(linkedin_city_tokens)
            if match_ratio > 0.5:
                return r['cbsa']
        cbsa = self.match_linkedin_city_to_cbsa_using_gpt_mapping(linkedin_city, linkedin_state)
        if not linkedin_state and not cbsa:
            return None
        if not cbsa:
            cbsa = self.match_linkedin_city_using_county(linkedin_city, linkedin_state)
        return cbsa

    def match_linkedin_city_using_county(self, linkedin_city, linkedin_state):
        try:
            county = self.search.by_city_and_state(linkedin_city, linkedin_state)[0].county
            cbsa = self.county_map.loc[(self.county_map['county'] == county) &
                                       (self.county_map['state_code'] == linkedin_state), 'cbsa'].item()
        except Exception as E:
            print(linkedin_city, E)
            return None
        return cbsa

    def match_linkedin_city_to_cbsa_using_gpt_mapping(self, linkedin_city, linkedin_state):
        if linkedin_state:
            gpt_mapping = self.gpt_map[self.gpt_map['state_code'] == linkedin_state]
        else:
            gpt_mapping = self.gpt_map
        city = gpt_mapping.loc[gpt_mapping['city'] == linkedin_city.lower(), 'cbsa'].reset_index(drop=True)
        if len(city) > 0:
            return city[0]
        return None

    def create_crosswalk_from_city_and_state_df(self, df, path_title):
        df['state_code'] = self.get_state_code(df['state'])
        df['cbsa'] = [self.match_linkedin_city_to_cbsa(x, y) for x, y in zip(df['city'], df['state_code'])]
        crosswalk = (df.merge(self.county_map.drop_duplicates(subset=['cbsa']),
                              on='cbsa', how='inner')[['id', 'cbsa', 'cbsa_code']])
        crosswalk.to_csv(f'{path_title}.csv', index=False)
        df.to_csv(f'{path_title}_analysis.csv', index=False)


def create_crosswalk_object():
    c = LinkedinCrosswalk()
    c.load_files()
    return c
