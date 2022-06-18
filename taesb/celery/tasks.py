""" 
Implement the Celery's tasks. 
""" 
from .celery import app 
from celery.signals import worker_process_init, worker_process_shutdown 
from celery.schedules import crontab 

from ..utils import CallbacksList 
from ..utils.DatabaseTask import DatabaseTask
# Database 
import psycopg2 

from .dml import INSERT_ANTS, \
        INSERT_ANTHILLS, \
        INSERT_FOODS, \
        INSERT_SCENARIOS, \
        BENCHMARKS

import time 

# Docs 
from typing import Dict 

DEBUG = True 

@app.task(base=DatabaseTask, bind=True, priority=9) 
def initialize_database(self, global_map: Dict): 
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
    cursor = self.db_conn.cursor() 
    cursor.execute(
            "\n".join(queries) 
    ) 
    cursor.close() 

def update_ants(cursor: psycopg2.extensions.cursor, global_map: Dict): 
    """ 
    Update the `ants` table. 
    """ 
    query = INSERT_ANTS(global_map["ants"]) 
    cursor.execute(query) 

def update_anthills(cursor: psycopg2.extensions.cursor, global_map: Dict): 
    """ 
    Update the `anthills` table. 
    """ 
    query = INSERT_ANTHILLS(global_map["anthills"], global_map["scenario_id"]) 
    cursor.execute(query) 

def update_scenarios(cursor: psycopg2.extensions.cursor, global_map: Dict): 
    """  
    Update the `scenarios` table. 
    """ 
    query = INSERT_SCENARIOS(global_map["scenario_id"], global_map["execution_time"], 
            global_map["active"]) 
    cursor.execute(query) 

def update_foods(cursor: psycopg2.extensions.cursor, global_map: Dict): 
    """ 
    Update the `foods` table. 
    """ 
    query = INSERT_FOODS(global_map["foods"], global_map["scenario_id"]) 
    cursor.execute(query) 

@app.task(base=DatabaseTask, bind=True) 
def update_db(self, global_map: Dict): 
    """ 
    Update the database. 
    """ 
    # Instantiate a cursor 
    cursor = self.db_conn.cursor() 
    update_scenarios(cursor, global_map) 
    update_anthills(cursor, global_map) 
    update_foods(cursor, global_map) 
    update_ants(cursor, global_map) 
    cursor.close() 

@app.task(base=DatabaseTask, bind=True)
def shutdown_db(self): 
    """ 
    Shutdown the database. 
    """ 
    self.db_conn.close() 

@app.task(base=BenchmarkTask, bind=True, priority=9) 
def benchmark(self, scenario_id): 
    """ 
    Compute the execution time for this pipeline. 
    """ 
    if self.start_time is None: 
        self.start(scenario_id, time.time()) 
    else: 
        self.current(scenario_id, time.time()) 
        
        # Insert the data for the current scenario in the database 
        query = BENCHMARKS(self.current_time, self.start_time) 
        # Send the data to the database 
        self.update_benchmarks(query) 

@app.task(base=DatabaseTask, bind=True, priority=1) 
def update_benchmark(self, query: str): 
    """ 
    Insert the benchmark data in the database. 
    """ 
    
@worker_process_shutdown.connect 
def shutdown_worker(**kwargs): 
    """ 
    Shutdown the workers. 
    """ 
    # Update data base access 
    print("Shutdown worker") 
    shutdown_db() 

