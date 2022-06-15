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



