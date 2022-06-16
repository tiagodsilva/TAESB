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
        DB_CREATE_GLOBAL, \
        DB_CREATE_LOCAL, \
        DB_CREATE_ATOMIC, \
        DROP_TABLES 

from .dml import INSERT_ANTS, \
        INSERT_ANTHILLS, \
        INSERT_FOODS, \
        INSERT_SCENARIOS 

import time 

# Docs 
from typing import Dict 

DEBUG = True 

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

#@app.task() 
#def current_foods(global_map: Dict): 
#    """
#    Compute the quantity of foods in each anthill. 
#    """ 
#    # Identify the anthills 
#    anthills = [anthill for anthill in global_map["anthills"]] 
#    # and the foods 
#    foods = [(anthill["name"], anthill["food_storage"]) for anthill in anthills] 
#    
#    # Return the quantity of foods in each anthill 
#    return foods 
#
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
            DB_CREATE_GLOBAL, 
            DB_CREATE_LOCAL, 
            DB_CREATE_ATOMIC 
    ] 
    
    app._init_database( 
            database="postgres", 
            user="tiago", 
            password="password", 
            queries=queries 
    ) 

@worker_process_shutdown.connect 
def shutdown_worker(**kwargs): 
    """ 
    Shutdown the workers. 
    """ 
    # Update data base access 
    print("Shutdown worker") 
    app.db_conn.close() 

