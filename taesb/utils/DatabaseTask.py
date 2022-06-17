""" 
Integrate Celery and Spark. 
"""
# Sys 
import os 
import sys 
import glob 

# Applications 
from celery import Task 
import psycopg2 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

# IO 
import warnings 

class DatabaseTask(Task): 
    """ 
    A class that assembles Celery and Spark. 
    """  
    _db_conn = None 

    @property 
    def db_conn(self): 
        """ 
        Return the access to the database. 
        """ 
        if self._db_conn is None: 
            # Persitent access to the data base 
            self._db_conn = psycopg2.connect( 
                    host=os.environ["POSTGRESQL_HOST"], 
                    database=os.environ["POSTGRESQL_DATABASE"], 
                    user=os.environ["POSTGRESQL_USER"],
                    password=os.environ["POSTGRESQL_PASSWORD"]
            ) 
            # Guarantee that the transactions' are autocommited 
            self._db_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)         
            # Use data base attributes 
       
        # Return the access to the database 
        return self._db_conn 

