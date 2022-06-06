""" 
Implement the Celery's tasks. 
""" 
from .celery import app 
from celery.signals import worker_process_init, worker_process_shutdown 
from celery.schedules import crontab 

from ..utils import CallbacksList 

import pyspark 
from pyspark.sql import SparkSession 
import pyspark.sql.functions as F 

# Database 
import psycopg2 
from .operational_db import DB_CREATE_SCENARIOS, \
        DB_CREATE_ANTHILLS, \
        DB_CREATE_ANTS, \
        DB_CREATE_FOODS, \
        VIEW_CREATE_STATS, \
        DROP_TABLES 

from .dml import INSERT_ANTS, \
        INSERT_ANTHILLS, \
        INSERT_FOODS, \
        INSERT_SCENARIOS 

import time 

# Docs 
from typing import Dict 

DEBUG = True 

# Periodic tasks 
app.conf.update( 
        {
            "beat_max_loop_interval": 5, 
            "beat_schedule": { 
                "update_stats": { 
                    "task": "taesb.celery.tasks.update_stats", 
                    "schedule": 5
                }
            } 
        } 
) 

@app.task() 
def initialize_database(global_map: Dict): 
    """ 
    Initialize the database, inserting the instances and their keys; subsequent queries 
    would be executed in parallel. 
    """ 
    scenario_id = global_map["scenario_id"] 
    # Queries in a consistent order 
    queries = [ 
            INSERT_SCENARIOS(scenario_id, global_map["execution_time"], global_map["active"]), 
            INSERT_ANTHILLS(global_map["anthills"], scenario_id), 
            INSERT_FOODS(global_map["foods"], scenario_id), 
            INSERT_ANTS(global_map["ants"]) 
    ] 
    print("\n".join(queries)) 
    # Execute each query 
    app.execute_query(
            "\n".join(queries) 
    ) 

@app.task() 
def update_ants(global_map: Dict): 
    """ 
    Update the `ants` table. 
    """ 
    query = INSERT_ANTS(global_map["ants"]) 
    app.execute_query(query) 

@app.task() 
def update_anthills(global_map: Dict): 
    """ 
    Update the `anthills` table. 
    """ 
    query = INSERT_ANTHILLS(global_map["anthills"], global_map["scenario_id"]) 
    app.execute_query(query) 

@app.task() 
def update_scenarios(global_map: Dict): 
    """  
    Update the `scenarios` table. 
    """ 
    query = INSERT_SCENARIOS(global_map["scenario_id"], global_map["execution_time"], 
            global_map["active"]) 
    app.execute_query(query) 

@app.task() 
def update_foods(global_map: Dict): 
    """ 
    Update the `foods` table. 
    """ 
    query = INSERT_FOODS(global_map["foods"], global_map["scenario_id"]) 
    app.execute_query(query) 

@app.task() 
def current_foods(global_map: Dict): 
    """
    Compute the quantity of foods in each anthill. 
    """ 
    # Identify the anthills 
    anthills = [anthill for anthill in global_map["anthills"]] 
    # and the foods 
    foods = [(anthill["name"], anthill["food_storage"]) for anthill in anthills] 
    
    # Return the quantity of foods in each anthill 
    return foods 

@worker_process_init.connect 
def init_worker(**kwargs): 
    """ 
    Instantiate a connection to the data base. 
    """ 
    print("Initializing connection to the database") 
    queries = list() 
    
    # If in debugging mode, drop tables 
    if DEBUG: 
        queries += [DROP_TABLES] 

    queries += [ 
            DB_CREATE_SCENARIOS, 
            DB_CREATE_ANTHILLS, 
            DB_CREATE_ANTS, 
            DB_CREATE_FOODS, 
            VIEW_CREATE_STATS 
    ] 
    
    app._init_database( 
            database="postgres", 
            user="tiago", 
            password="password", 
            queries=queries 
    ) 

@app.task() 
def update_stats(): 
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
        anthills = tables["scenarios"].count() 

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
    

