# bea_etl
Extract, transform, and load data using the Bureau of Economic Analysis API

This tool will combine the BEA Api, Asyncio, and threading to create an efficient ETL pipeline in Python.

## BEAAPI

BEAAPI is a python library for easy BEA data extraction. It can be installed with `pip`. The library requires an API key, available from [bea.gov's sign up page](https://apps.bea.gov/api/signup/).

End points and documentation are available in the [user guide](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf), and interactive data sets are also available on [BEA's website](https://www.bea.gov/data) 

## Asyncio

Asyncio allows for asynchronous execution of python code, useful for IO bound tasks. In this example, Asyncio will be combined with Aiohttp to extract data from BEA's API for a hundred endpoints related to employee compensation, personal consumption expenditures, personal income, & disposable income. 

## Threading

Threading in Python allows for different parts of the code to run concurrently, useful for IO bound tasks. In this example, a pool of threads will be executed with `ThreadPoolExecutor` in order to write JSON files for the cleaned BEA data. With PostgreSQL, JSON files can be loaded in databases for easy querying and analysis.

## Commands

To get clean data as JSON files, run the command line tool:

    python3 clean_bea_data.py

