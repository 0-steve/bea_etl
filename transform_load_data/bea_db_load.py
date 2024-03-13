import bea_data_prep as prep
import duckdb as db

class db_load():
    def __init__(self):
        bea_data = prep.bea_data_prep()
        self.arrow_dict = bea_data.transform()

    def validate_null(self, table):
        # code, timeperiod, table, endpoint, topic, geofips, geoname cannot be null
        query = f"""
            SELECT count(*) as null_records
            FROM {table} 
            WHERE code IS NULL OR 
                geofips IS NULL OR
                geoname IS NULL OR
                timeperiod IS NULL OR
                'table' IS NULL OR
                endpoint IS NULL OR
                topic IS NULL
                 """
        return query

    def validate_geo(self, table):
        # at least 50 unique geofips & 50 unique geoname should exist. Some tables will include United States and/or District of Columbia
        query = f"""
            SELECT 
                count(distinct geofips) as geofips_count,
                count(distinct geoname) as geoname_count
            FROM {table} 
                 """
        return query

    def duckdb(self):
        # create DuckDB database
        con = db.connect("bureau_economic_analysis.db")

        # create table
        for table_name, data in self.arrow_dict.items():
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM data")
            print(f"Created table {table_name} in bureau_economic_analysis database")
            print(f"Schema for {table_name}")
            query = f"SHOW {table_name}"
            print(con.sql(query))
            print(f"Validate null values don't exist in code, geofips, geoname, timeperiod, table, endpoint, topic columns")

            validate_null_query = self.validate_null(table_name)
            print(con.sql(validate_null_query))

            print(f"Validate at least 50 unique geofips & geonames exist")
            validate_geo_query = self.validate_geo(table_name)
            print(con.sql(validate_geo_query))
            print()
        
        con.execute("EXPORT DATABASE 'db'")

        con.close()

if __name__ == "__main__":
    from datetime import datetime
    
    startTime = datetime.now()
    load = db_load()
    load.duckdb()
    print("Run time", datetime.now() - startTime)