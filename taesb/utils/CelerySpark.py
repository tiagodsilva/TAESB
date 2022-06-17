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
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

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
    
        # Insert the application name 
        self.app_name = kwargs["main"] 
 
    def _init_database(self, host: str, database: str, user: str, password: str): 
        """ 
        Initialize the workers, with the Spark attributes. 
        """ 
        self.db_conn = psycopg2.connect( 
                host=host, 
                database=database, 
                user=user,
                password=password 
        ) 
        # Guarantee that the transactions' are autocommited 
        self.db_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)         
        # Use data base attributes 
        self.database = database 

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
