""" 
Integrate Celery and Spark. 
"""
# Sys 
import os 
import sys 
import glob 

# Applications 
from celery import Celery 
import pyspark 
from pyspark.sql import SparkSession, \
        functions as F 

import psycopg2 

# IO 
import warnings 

class CeleryPostgres(Celery): 
    """ 
    A class that assembles Celery and Spark. 
    """ 

    def __init__(self, *args, **kwargs): 
        """ 
        Constructor method for `CelerySpark`. 
        """ 
        # Call the super constructor 
        super().__init__(*args, **kwargs) 
    
        # Attributes to combine data from the PostgreSQL data base 
        self.db_conn = None 
        self.spark_session = None 
        # The name of the database 
        self.database = None 
        self.db_url = None 

        # Insert the application name 
        self.app_name = kwargs["main"] 

    def _init_database(self, database: str, user: str, password: str, 
            queries: str): 
        """ 
        Initialize the workers, with the Spark attributes. 
        """ 
        self.db_conn = psycopg2.connect( 
                database=database, 
                user=user,
                password=password 
        ) 
        
        # Use data base attributes 
        self.database = database 
        self.db_url = "jdbc:postgresql://localhost:5432/{database}".format(database=database) 

        # Execute the queries 
        self.execute_query("\n".join(queries)) 

    def execute_query(self, query: str): 
        """ 
        Execute a query in the data base pointed by `db_conn`. 
        """ 
        # Instantiate a cursor 
        cur = self.db_conn.cursor() 
        # Execute a query 
        cur.execute(query) 
        # and commit the changes 
        cur.close() 
        self.db_conn.commit() 


def update_stats(app):
    """
    Update the appropriate data in the analytical database.
    """
    # print("Update the database, Luke!")
    # Read the tables
    tables_names = ["ants", "anthills", "foods", "scenarios"]

    # And instantiate a container for the data frames
    tables = {
            dbtable:None for dbtable in tables_names
    }

    # Iterate across the tables
    for table in tables:
        tables[table] = app.read_table(table)

    # print(tables)

    # Compute the desired statistics
    try:
        scenarios = tables["scenarios"].count()
        anthills = tables["anthills"].count()

        # Quantity of foods available at the food deposits
        foods_in_deposit = tables["foods"] \
                .agg(F.sum("current_volume")) \
                .collect()[0][0]

        # Quantity of ants alive
        ants = tables["ants"].count()
        # Quantity of ants searching for food
        ants_searching_food = tables["ants"] \
                .agg(F.sum("searching_food")) \
                .collect()[0][0]

        # Quantity of foods in transit
        foods_in_transit = ants - ants_searching_food
        # Quantity of foods in the anthills
        foods_in_anthills = tables["anthills"] \
                .agg(F.sum("food_storage")) \
                .collect()[0][0]

        # Quantity of foods in total
        total_foods = foods_in_deposit + foods_in_transit + foods_in_anthills

        # Active scenarios
        active_scenarios = tables["scenarios"] \
            .filter(tables["scenarios"].active != 1)

        avg_execution_time = active_scenarios.agg(
                F.mean("execution_time")
        ) \
        .collect()[0][0]

        # Execution time in the scenarios
        ord_scenarios = active_scenarios \
                .orderBy(F.desc("execution_time"))

        fst_scenario = ord_scenarios \
                .take(1)[0] \
                .asDict()

        slw_scenario = ord_scenarios \
                .tail(1)[0] \
                .asDict()

        # print(fst_scenario, slw_scenario)

        fst_scenario_id, fst_scenario_time = fst_scenario["scenario_id"], \
                fst_scenario["execution_time"]
        slw_scenario_id, slw_scenario_time = slw_scenario["scenario_id"], \
                slw_scenario["execution_time"]

        # Ant's food gathering
        ants_foods = tables["ants"] \
                .agg(F.avg("captured_food"), F.max("captured_food")) \
                .collect()[0]

        avg_ant_food, max_ant_food = ants_foods[0], ants_foods[1]

        # Update the data base
        app.update_stats(
                scenarios=scenarios,
                anthills=anthills,
                ants_searching_food=ants_searching_food,
                ants=ants,
                foods_in_anthills=foods_in_anthills,
                foods_in_deposit=foods_in_deposit,
                avg_execution_time=avg_execution_time,
                fst_scenario_id=fst_scenario_id,
                fst_scenario_time=fst_scenario_time,
                slw_scenario_id=slw_scenario_id,
                slw_scenario_time=slw_scenario_time,
                avg_ant_food=avg_ant_food,
                max_ant_food=max_ant_food
        )

        # print(total_foods)
    except TypeError as err:
        # There are no instances in the table
        # print(err)
        pass
    except IndexError as err:
        # There are no instances in the anthill tables
        # print(err)
        pass
