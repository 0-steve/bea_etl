import pandas as pd
from functools import lru_cache
import json
import re
from concurrent.futures import ThreadPoolExecutor
import os

from bea_async import bea_api


class bea_data_clean():
    """
    Region wide data from BEA includes state level, country level, and regional level full names. 
    Reference Wikipedia to collect state + country names (50 states + DC + US).
    """
    @lru_cache
    def get_state_names(self):
        wiki_url = "https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States"
        wiki_states = pd.read_html(wiki_url)
        dc = (wiki_states[2].iloc[0, 0],)
        us = (re.search("United States", wiki_states[0][0][1]).group(0) + " *", ) # us code for country level data
        states = tuple(state.replace("[B]", "") for state in wiki_states[1].droplevel(0, axis=1).iloc[:50, 0]) # remove annotations in name
        return us + states + dc
    
    @lru_cache
    def collect_bea_data(self, key):
        """
        Collect API data from bea_async library
        """
        async_api = bea_api(key)
        results =  async_api.collect_data()
        return results
    
    def state_filter(self, data_dict, bea_variable):
        """
        Filter data for region names desired from Wikipedia and write data as JSON files
        """
        df_joined = pd.concat(data_dict.values()).reset_index(drop=True)
        df_joined_states = df_joined[df_joined.GeoName.isin(self.get_state_names())]
        df_joined_states = df_joined_states.to_json()
        file_path = os.path.join("data", f"{bea_variable}.json")
        with open(file_path, "w") as outfile:
            json.dump(df_joined_states, outfile)
        outfile.close()

    def write_files(self, bea_data_dict):
        """
        The function to be run in each of the pools. Reference the linecode in the data to know which BEA data is being used.
        """
        if "SAGDP4N-1" in bea_data_dict.keys():
            print("Writing BEA compensation data as JSON")
            bea_data.state_filter(bea_data_dict, "compensation")
        elif "SAPCE2-1" in bea_data_dict.keys():
            print("Writing BEA consumption data as JSON")
            bea_data.state_filter(bea_data_dict, "consumption_expenditures")
        elif "SAINC30-10" in bea_data_dict.keys():
            print("Writing BEA personal income data as JSON")
            bea_data.state_filter(bea_data_dict, "personal_income")
        elif "SAINC51-51" in bea_data_dict.keys():
            print("Writing BEA disposable income data as JSON")
            bea_data.state_filter(bea_data_dict, "disposable_income")
        elif "CAINC4-1" in bea_data_dict.keys():
            print("Writing BEA employment data as JSON")
            bea_data.state_filter(bea_data_dict, "employment")
        elif "CAINC30-10" in bea_data_dict.keys():
            print("Writing BEA wages & salaries data as JSON")
            bea_data.state_filter(bea_data_dict, "wages_salary")
        elif "CAINC1-1" in bea_data_dict.keys():
            print("Writing BEA population data as JSON")
            bea_data.state_filter(bea_data_dict, "population")
        elif "CAGDP9-1" in bea_data_dict.keys():
            print("Writing BEA real GDP data as JSON")
            bea_data.state_filter(bea_data_dict, "real_gdp")
        elif "SAGDP2N-1" in bea_data_dict.keys():
            print("Writing BEA GDP data as JSON")
            bea_data.state_filter(bea_data_dict, "gdp")

    def file_save_threads(self, bea_data):
        """
        Execute multiple pools for the thread to run write_file functions 
        """
        with ThreadPoolExecutor() as executor:
            executor.map(self.write_files, bea_data)

if __name__ == "__main__":
    from datetime import datetime
    
    startTime = datetime.now()

    key = os.environ.get("BEA_KEY")
    bea_data = bea_data_clean()
    data = bea_data.collect_bea_data(key)
    bea_data.file_save_threads(data)

    print("BEA data saved as JSON in data folder")
    print()
    print("Run time", datetime.now() - startTime)
