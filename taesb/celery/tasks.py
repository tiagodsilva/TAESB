""" 
Implement the Celery's tasks. 
""" 
from .celery import app 
from celery.signals import worker_process_init, worker_process_shutdown 
from celery.schedules import crontab 

from ..utils import CallbacksList 
# SPARK_* files 
from ..SparkConf import * 

import pyspark 
from pyspark.sql import SparkSession 
import pyspark.sql.functions as F 

# Database 
import psycopg2 

from .dml import INSERT_ANTS, \
        INSERT_ANTHILLS, \
        INSERT_FOODS, \
        INSERT_SCENARIOS 

import time 

# Docs 
from typing import Dict 

DEBUG = True 

@app.task(priority=9) 
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

@app.task(priority=5) 
def update_ants(global_map: Dict): 
    """ 
    Update the `ants` table. 
    """ 
    query = INSERT_ANTS(global_map["ants"]) 
    app.execute_query(query) 

@app.task(priority=6) 
def update_anthills(global_map: Dict): 
    """ 
    Update the `anthills` table. 
    """ 
    query = INSERT_ANTHILLS(global_map["anthills"], global_map["scenario_id"]) 
    app.execute_query(query) 

@app.task(priority=8) 
def update_scenarios(global_map: Dict): 
    """  
    Update the `scenarios` table. 
    """ 
    query = INSERT_SCENARIOS(global_map["scenario_id"], global_map["execution_time"], 
            global_map["active"]) 
    app.execute_query(query) 

@app.task(priority=7) 
def update_foods(global_map: Dict): 
    """ 
    Update the `foods` table. 
    """ 
    query = INSERT_FOODS(global_map["foods"], global_map["scenario_id"]) 
    app.execute_query(query) 

@worker_process_init.connect 
def init_worker(**kwargs): 
    """ 
    Instantiate a connection to the data base. 
    """ 
    print("Initializing connection to the database") 
    app._init_database( 
            host=POSTGRESQL_HOST, 
            database=POSTGRESQL_DATABASE, 
            user=POSTGRESQL_USER, 
            password=POSTGRESQL_PASSWORD, 
    ) 

@worker_process_shutdown.connect 
def shutdown_worker(**kwargs): 
    """ 
    Shutdown the workers. 
    """ 
    # Update data base access 
    print("Shutdown worker") 
    app.db_conn.close() 

