""" 
Implement the Celery's tasks. 
""" 
from .celery import app 
from celery.signals import worker_process_init, worker_process_shutdown 
from celery.schedules import crontab 

# Database 
import psycopg2 
from .queries import DB_CREATE_SCENARIOS, \
        DB_CREATE_ANTHILLS, \
        DB_CREATE_ANTS, \
        DB_CREATE_FOODS, \
        DROP_TABLES, \
        INSERT_ANTS, \
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
            "beat_max_loop_interval": 1, 
            "beat_schedule": { 
                "update_stats": { 
                    "task": "taesb.celery.tasks.update_stats", 
                    "schedule": 5
                }
            } 
        } 
) 


def execute_query(query: str): 
    """ 
    Execute a query in the database. 
    """ 
    # Instantiate a cursor 
    cur = db_conn.cursor() 
    # Execute the query 
    cur.execute(query) 
    # and commit the changes 
    cur.close() 
    db_conn.commit() 

@app.task() 
def initialize_database(global_map: Dict): 
    """ 
    Initialize the database, inserting the instances and their keys; subsequent queries 
    would be executed in parallel. 
    """ 
    scenario_id = global_map["scenario_id"] 
    # Queries in a consistent order 
    queries = [ 
            INSERT_SCENARIOS(scenario_id, global_map["execution_time"]), 
            INSERT_ANTHILLS(global_map["anthills"], scenario_id), 
            INSERT_FOODS(global_map["foods"], scenario_id), 
            INSERT_ANTS(global_map["ants"]) 
    ] 
    print("\n".join(queries)) 
    # Execute each query 
    execute_query(
            "\n".join(queries) 
    ) 

@app.task() 
def update_ants(global_map: Dict): 
    """ 
    Update the `ants` table. 
    """ 
    query = INSERT_ANTS(global_map["ants"]) 
    execute_query(query) 

@app.task() 
def update_anthills(global_map: Dict): 
    """ 
    Update the `anthills` table. 
    """ 
    query = INSERT_ANTHILLS(global_map["anthills"], global_map["scenario_id"]) 
    execute_query(query) 

@app.task() 
def update_scenarios(global_map: Dict): 
    """  
    Update the `scenarios` table. 
    """ 
    query = INSERT_SCENARIOS(global_map["scenario_id"], global_map["execution_time"]) 
    execute_query(query) 

@app.task() 
def update_foods(global_map: Dict): 
    """ 
    Update the `foods` table. 
    """ 
    query = INSERT_FOODS(global_map["foods"], global_map["scenario_id"]) 
    execute_query(query) 

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

# Instantiate a connection to the database for 
# each worker 
db_conn = None 

@worker_process_init.connect 
def init_worker(**kwargs): 
    """ 
    Instantiate a connection to the data base. 
    """ 
    global db_conn 
    print("Initializing connection to the database") 
    db_conn = psycopg2.connect( 
            database="postgres",
            user="tiago",
            password="password" 
    ) 
    # Queries to execute 
    queries = list() 
    
    # If in debugging mode, drop tables 
    if DEBUG: 
        queries += [DROP_TABLES] 

    queries += [ 
            DB_CREATE_SCENARIOS, 
            DB_CREATE_ANTHILLS, 
            DB_CREATE_ANTS, 
            DB_CREATE_FOODS 
    ] 
    
    # Execute the joint query 
    execute_query("\n".join(queries)) 

    # Commit the updates to the database 
    db_conn.commit() 

@app.task() 
def update_stats(): 
    """ 
    Update the appropriate data in the analytical database. 
    """ 
    print("Update the database, Luke!") 

