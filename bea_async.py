import beaapi as bea
import re
import asyncio
import aiohttp
from functools import lru_cache

class bea_api(): 
    def __init__(self, api_key): 
        """ 
        api_key = api key needed for census api
        """
        self.key = api_key
        self.table_dict = {"SAINC30": "Personal income", 
                           "SAGDP4N": "Employee compensation", 
                           "SAPCE2": "Personal consumption expenditures", 
                           "SAINC51": "Disposable income", 
                           "SAGDP2N": "GDP", 
                           "CAGDP9": "Real GDP", 
                           "CAINC1": "Population", 
                           "CAINC30": "Wages", 
                           "CAINC4": "Employment"}

    def param_vals(self, dataset, param):
        return bea.get_parameter_values(self.key, datasetname=dataset, parametername=param)

    @lru_cache
    def linecode_lookup(self):
        """
        Line codes represent different linecode endpoints available from BEA.
        """
        linecodes = self.param_vals("Regional", "LineCode")
        linecodes["table"] = linecodes.Desc.apply(lambda x: re.search("(?<=\[).+(?=\])", x).group(0))
        linecodes["Desc"] = linecodes.Desc.apply(lambda x: x.split("] ")[1])
        return linecodes

    async def get_state_linecodes(self, table, line_code):
        """
        Call BEA API to get region wide data for the provided table and line code.
        """
        return bea.get_data(self.key, datasetname="Regional", TableName=table, LineCode=line_code, GeoFips="STATE", year="ALL")
    
    def get_bea_keys(self, table):
        """
        Collect all line codes for the given table. Some tables may require additional cleaning.
        """
        linecodes = self.linecode_lookup()
        keys = linecodes[(linecodes.table==table)].drop_duplicates()
        if table == "SAGDP4N":
            return tuple(k for k in keys.Key.unique() if not re.search("\(", k)) # get keys for industries with a parenthesis
        else:
            return tuple(keys.Key)
        
    async def get_bea_data(self, table, bea_session):
        """
        Aysnc calls for provided BEA table + linecode
        """
        disposable_income_keys = self.get_bea_keys(table) 
        disposable_income_results = {}
        async with bea_session as session:
            print("Collecting BEA disposable income data")
            for k in disposable_income_keys:
                key_results = await self.get_state_linecodes(table, k)
                disposable_income_results[f"{table}-{k}"] = key_results
                print(f"{self.table_dict[table]} endpoint {k} found")
            print(f"BEA {self.table_dict[table]} data collected")
            print()
            return disposable_income_results

    async def async_bea_api(self):
        """
        Run all async functions
        """
        async with aiohttp.ClientSession() as session:
            task_results = await asyncio.gather(*(self.get_bea_data("SAGDP4N", session), 
                                                  self.get_bea_data("SAPCE2", session), 
                                                  self.get_bea_data("SAINC30", session),
                                                  self.get_bea_data("SAINC51", session),
                                                  self.get_bea_data("SAGDP2N", session),
                                                  self.get_bea_data("CAGDP9", session),
                                                  self.get_bea_data("CAINC1", session),
                                                  self.get_bea_data("CAINC30", session)),
                                                  self.get_bea_data("CAINC4", session))
            return task_results

    def collect_data(self):
        print("Collecting BEA data:")
        for val in self.table_dict.values():
            print(val)
        print()
        results = asyncio.run(self.async_bea_api()) 

        print("BEA async complete")
        print()
        return results

