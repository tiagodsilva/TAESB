""" 
Implement the Celery's tasks. 
""" 
from .celery import app 
from celery.signals import worker_process_init, worker_process_shutdown 

# Database 
import psycopg2 
from .queries import DB_CREATE_SCENARIOS, \
        DB_CREATE_ANTHILLS, \
        DB_CREATE_ANTS, \
        DB_CREATE_FOODS, \
        DROP_TABLES 
import time 

# Docs 
from typing import Dict 

DEBUG = True 

@app.task() 
def current_foods(global_map: Dict): 
    """
    Compute the quantity of foods in each anthill. 
    """ 
    # Identify the anthills 
    anthills = [anthill for anthill in global_map["anthills"]] 
    # and the foods 
    foods = [anthill["food_storage"] for anthill in anthills]
    
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
    cur = db_conn.cursor() 
    
    if DEBUG: 
        cur.execute(DROP_TABLES) 

    # Execute a query to create the tables 
    for db in [DB_CREATE_SCENARIOS, DB_CREATE_ANTHILLS, DB_CREATE_ANTS, DB_CREATE_FOODS]: 
        cur.execute(db) 
        time.sleep(1) 
    # Commit the updates to the database 
    db_conn.commit() 
