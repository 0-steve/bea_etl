# bea_etl
Extract, transform, and load data using the Bureau of Economic Analysis API

This tool will combine the BEA Api, Asyncio, threading, multiprocessing, and DuckDB to create an efficient ETL pipeline in Python.

![Alt text](/img/etl_flow.png?raw=true")

## BEA API

BEAAPI is a python library for easy BEA data extraction. It can be installed with `pip`. The library requires an API key, available from [bea.gov's sign up page](https://apps.bea.gov/api/signup/).

End points and documentation are available in the [user guide](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf), and interactive data sets are also available on [BEA's website](https://www.bea.gov/data) 

## Extract tools

### Asyncio

Asyncio allows for asynchronous execution of python code, useful for IO bound tasks. In this example, Asyncio will be combined with Aiohttp to extract data from BEA's API for a hundred endpoints related to employee compensation, personal consumption expenditures, personal income, disposable income, employment, population, GDP, real GDP, and wages + salaries.

Install [Asyncio](https://pypi.org/project/asyncio/) & [Aiohttp](https://pypi.org/project/aiohttp/) with `pip`

### Threading

Threading in Python allows for different parts of the code to run concurrently, useful for IO bound tasks. In this example, a pool of threads will be executed with `ThreadPoolExecutor` in order to write JSON files for the cleaned BEA data. With PostgreSQL, JSON files can be loaded in databases for easy querying and analysis.

## Transform tools

### Multiprocessing

Similar to threading, multiprocessing allows for concurrent runs of your code and is useful for CPU intensive tasks. In this example, 4 pools are used to process, transform, and join JSON data. This ultimatley preps the data to be ready to be loaded into a database.

## Load tools

### DuckDB

DuckDB is a serverless database tool that can handle data in many formats. It looks you to create, alter, and query data quickly & efficiently. BEA tables are created from the original API call in the extract step, and further analysis can be run with PostgreSQL.

## Commands

To extract API data as JSON files, run the command line tool:

    python3 extract_data/bea_data_json.py

To clean and load JSON data into DuckDB, run the command line tool:

    python3 transform_load_data/bea_db_load.py

