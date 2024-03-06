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

    def param_vals(self, dataset, param):
        return bea.get_parameter_values(self.key, datasetname=dataset, parametername=param)

    @lru_cache
    def linecode_lookup(self):
        """
        Line codes represent different endpoints available from BEA.
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
            return tuple(k for k in keys.Key.unique() if not re.search("\(", k)) # get keys for industries without a parenthesis
        else:
            return tuple(keys.Key)
    
    async def bea_compensation(self, bea_session):
        """
        Aysnc calls for every compensation line code
        """
        compensation_keys = self.get_bea_keys("SAGDP4N")
        compensation_results = {}
        async with bea_session as session:
            print("Collecting BEA compensation data")
            for k in compensation_keys:
                key_results = await self.get_state_linecodes("SAGDP4N", k)
                compensation_results[f"SAGDP4N-{k}"] = key_results
                print("Compensation key found", k)
            print("BEA compensation data collected")
            print()
            return compensation_results
    
    async def bea_consumption(self, bea_session):
        """
        Aysnc calls for every consumption line code
        """
        consumption_keys = self.get_bea_keys("SAPCE2")
        consumption_results = {}
        async with bea_session as session:
            print("Collecting BEA consumption data")
            for k in consumption_keys:
                key_results = await self.get_state_linecodes("SAPCE2", k)
                consumption_results[f"SAPCE2-{k}"] = key_results
                print("Consumption key found", k)
            print("BEA consumption data collected")
            print()
            return consumption_results

    async def bea_personal_income(self, bea_session):
        """
        Aysnc calls for every personal income line code
        """
        personal_income_keys = self.get_bea_keys("SAINC30")
        personal_income_results = {}
        async with bea_session as session:
            print("Collecting BEA personal income data")
            for k in personal_income_keys:
                key_results = await self.get_state_linecodes("SAINC30", k)
                personal_income_results[f"SAINC30-{k}"] = key_results
                print("Personal income key found", k)
            print("BEA personal income data collected")
            print()
            return personal_income_results

    async def bea_disposable_income(self, bea_session):
        """
        Aysnc calls for every disposable income line code
        """
        disposable_income_keys = self.get_bea_keys("SAINC51")
        disposable_income_results = {}
        async with bea_session as session:
            print("Collecting BEA disposable income data")
            for k in disposable_income_keys:
                key_results = await self.get_state_linecodes("SAINC51", k)
                disposable_income_results[f"SAINC51-{k}"] = key_results
                print("Disposable income key found", k)
            print("BEA disposable income data collected")
            print()
            return disposable_income_results

    async def async_bea_api(self):
        """
        Run all async functions
        """
        async with aiohttp.ClientSession() as session:
            task_results = await asyncio.gather(*(self.bea_compensation(session), 
                                                  self.bea_consumption(session), 
                                                  self.bea_personal_income(session),
                                                  self.bea_disposable_income(session)))
            return task_results
    
    def collect_data(self):
        print("Collecting BEA data for compensation of employees, personal consumption expenditures, personal income, & disposable income")
        print()
        results = asyncio.run(self.async_bea_api()) 

        print("BEA async complete")
        print()
        return results

