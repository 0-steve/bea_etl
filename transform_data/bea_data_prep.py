import pandas as pd
import pyarrow as pa
import os
import re
import json
from collections import defaultdict
import multiprocessing as mp

pd.options.mode.chained_assignment = None  

class bea_data_prep():
    def __init__(self):
        self.data_path = "../extract_data/data"
        self.endpoints_df = self.endpoints_file()

    def find_json_paths(self):
        path_dict = {}
        for file in os.listdir(self.data_path):
            if ".json" in file:
                path = os.path.join(self.data_path, file)
                table = re.search("(_data\\/data\\/)(.*)(?=\\.json)", path).group(2)
                path_dict[table] = path
        return path_dict

    def clean_json_files(self, table, path):
        data_dict = {}
        data_df = self.bea_dataframe(path)
        clean_endpoints_df = self.clean_endpoints(data_df)
        description_df = self.table_description(table, clean_endpoints_df)
        endpoints_description_df = clean_endpoints_df.merge(description_df[["endpoint", "table", "topic"]], 
                                                            how="inner", 
                                                            on = ["table", "endpoint"]).drop_duplicates().reset_index(drop=True)
        data_endpoints_df = data_df.merge(endpoints_description_df, how="inner", on="code")
        data_endpoints_table = pa.Table.from_pandas(data_endpoints_df)
        data_dict[table] = data_endpoints_table
        return data_dict
    
    def endpoints_file(self):
        endpoints = pd.read_csv("inputs/endpoints.csv", dtype="str")
        endpoints.rename({"Key": "endpoint", "Desc": "desc"}, axis=1, inplace=True)
        return endpoints
    
    def bea_dataframe(self, path):
        json_file = open(path, "r")
        json_data = json.load(json_file)
        json_dict = json.loads(json_data)
        json_df = pd.DataFrame.from_dict(json_dict)
        json_df["GeoName"] = json_df["GeoName"].apply(lambda x: "United States" if x == "United States *" else x)
        json_df.columns =  map(str.lower, json_df.columns)
        return json_df
    
    def clean_endpoints(self, df):
        code_dict = defaultdict(lambda: defaultdict(str))
        for code in df.code.unique():
            code_components = code.split("-")
            code_dict[code]["table"] = code_components[0]
            code_dict[code]["endpoint"] = code_components[1]
        return pd.DataFrame(code_dict).T.reset_index(names="code")
    
    def table_description(self, table, description_df):
        endpoint_re_map = {
            "compensation": (["Compensation of employees: ", " \(\d.+\)$"], ["", ""]),
            "real_gdp": (["Real GDP: ", " \(\d.+\)$"], ["", ""]),
            "gdp": (["Gross domestic product \(GDP\) by state\: ",  " \(\d.+\)$"], ["", ""]),
            "consumption_expenditure": (["Per capita personal consumption expenditures: "], ["", ""])
        }

        if table in endpoint_re_map:
            endpoint_re = endpoint_re_map[table]
            endpoints_description = self.endpoints_df [self.endpoints_df.table.isin(description_df.table.unique())]
            endpoints_description["topic"] = endpoints_description.desc.replace(endpoint_re[0], endpoint_re[1], regex=True)
            return endpoints_description
        else:
            endpoints_description = self.endpoints_df [self.endpoints_df.table.isin(description_df.table.unique())]
            endpoints_description["topic"] = endpoints_description["desc"]
            return endpoints_description
        
if __name__ == "__main__":
    from datetime import datetime
    
    startTime = datetime.now()

    bea_prep = bea_data_prep()
    path_dict = bea_prep.find_json_paths()
    path_args = tuple(map(tuple, path_dict.items()))

    pool = mp.Pool(processes=4)
    results = pool.starmap(bea_prep.clean_json_files, path_args)
    print(results)

    print("BEA JSON data prepped for loading.")
    print()
    print("Run time", datetime.now() - startTime)