class bea_views():
    def __init__(self, con):
        self.con = con

    def create_consumer_expenditure_view(self):
        ce_topics = ("Per capita personal consumption expenditures: Nondurable goods", 
                    "Per capita personal consumption expenditures: Durable goods", 
                    "Per capita personal consumption expenditures: Services",
                    "Per capita personal consumption expenditures: Housing and utilities")
        query = f""" 
        create or replace view consumer_expenditures as 

        with expenditures as (

        select
            geoname as state,
            timeperiod as year,
            datavalue as spend,
            lag(datavalue, 1) over (partition by geoname, topic order by timeperiod asc) as prev_year_spend,
            round(100 * (datavalue - lag(datavalue, 1) over (partition by geoname, topic order by timeperiod asc)) / datavalue, 2) as spend_change,
            case 
                when topic like '%Nondurable%' then 'nondurable goods'
                when topic like '%Durable%' then 'durable goods'
                when topic like '%Services%' then 'services'
                when topic like '%Housing%' then 'housing'
            end as consumer_expenditure
        from consumption_expenditures
        where topic in {ce_topics}
            and timeperiod >= '2000'
        order by timeperiod,
            topic,
            geoname)

        select
            state,
            year,
            spend,
            prev_year_spend,
            spend_change,
            dense_rank() over (partition by year, consumer_expenditure order by spend_change desc) as change_rank,
            consumer_expenditure
        from expenditures
        order by year,
            consumer_expenditure,
            state
                """
        self.con.execute(query)
        print("Created Consumer Expenditures view")

    def create_income_view(self):
        query = f""" 
        create or replace view income as

        with all_income as (

        select
            geoname as state,
            timeperiod as year,
            datavalue as income,
            topic
        from disposable_income
        where topic = 'Per capita disposable personal income'
            and timeperiod >= 2000

        union

        select
            geoname as state,
            timeperiod as year,
            datavalue as income,
            topic
        from employment
        where topic = 'Per capita personal income'
            and timeperiod >= 2000
        order by timeperiod,
            topic,
            geoname),

        income_change as (

        select
            state,
            year,
            income,
            lag(income, 1) over (partition by state, topic order by year asc) as prev_year_income,
            round(100 * (income - lag(income, 1) over (partition by state, topic order by year asc)) / income, 2) as income_change,
            case
                when topic like '%disposable%' then 'disposable income'
                when topic like '%personal%' then 'personal income'
            end as income_type
        from all_income
        order by year,
            topic,
            state)

        select
            state,
            year,
            income,
            prev_year_income,
            income_change,
            dense_rank() over (partition by year, income_type order by income_change desc) as change_rank,
            income_type
        from income_change
        order by year,
            income_type,
            state
                """
        self.con.execute(query)
        print("Created Income view")

    def create_employment_view(self):
        query = f""" 
        create or replace view employment_population as

        select
            employment.geoname as state,
            employment.timeperiod as year,
            employment.datavalue as employment,
            population.datavalue as population,
            employment.topic
        from wages_salary employment
        inner join wages_salary population on employment.geoname = population.geoname and employment.timeperiod = population.timeperiod
        where employment.topic = 'Total employment'
            and employment.timeperiod >= 2000
            and population.topic = 'Population'
        order by state,
            year
                """
        self.con.execute(query)
        print("Created employment view")

    def create_compensation_view(self):
        industries_to_remove = ("All industry total", "Private services-providing industries", "Private industries")
        query = f""" 
        create or replace view industry_compensation as

        with compensation_ranked as (
            select
                geoname as state,
                timeperiod as year,
                datavalue as compensation,
                topic as industry,
                dense_rank() over (partition by state, year order by datavalue desc) as compensation_rank
            from compensation 
            where geoname != 'United States'
                and topic not in {industries_to_remove})

        select
            comp.geoname as state,
            comp.timeperiod as year,
            comp.datavalue as compensation,
            comp.topic as industry,
            ranked.industry as top_industry
        from compensation comp
        inner join compensation_ranked ranked on comp.geoname = ranked.state and comp.timeperiod = ranked.year
        where comp.geoname != 'United States'
            and topic not in {industries_to_remove}
            and ranked.compensation_rank = 1
        order by 
            comp.geoname, 
            comp.timeperiod
                """
        self.con.execute(query)
        print("Created compensation view")

    def views_exist(self):
        db_views = set(self.con.sql("select view_name from duckdb_views").df()["view_name"].to_list())
        create_view = {"income": self.create_income_view,
                       "employment_population": self.create_employment_view, 
                       "consumer_expenditures": self.create_consumer_expenditure_view, 
                       "industry_compensation": self.create_compensation_view}

        views_needed = set(list(create_view.keys()))
        views_to_create = tuple(db_views ^ views_needed)

        if len(views_to_create) > 0:
            print("Need to create views for:", views_to_create)
            for topic in views_to_create:
                create_view[topic]()
