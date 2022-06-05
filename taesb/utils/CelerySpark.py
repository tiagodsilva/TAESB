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
from pyspark.sql import SparkSession 

class CelerySpark(Celery): 
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
        
        # Insert the application name 
        self.app_name = kwargs["main"] 

    def init_worker(database: str, user: str, password: str, 
            queries: str, app_name: str): 
        """ 
        Initialize the workers, with the Spark attributes. 
        """ 
        self.db_conn = pycopg2.connect( 
                database=database, 
                user=user,
                password=password 
        ) 
        
        # Execute the queries 
        execute_query("\n".join(queries)) 

        # Get (or create) the current Spark session 
        self.spark_session = SparkSession \
                .builder \
                .appName(self.app_name) 
                .config("spark.jars", "postgresql-42.3.6.jar") \
                .getOrCreate() 

        # Update spark's logging 
        sc = pyspark.SparkContext.getOrCreate() 
        sc.setLogLevel("FATAL") 

    def execute_query(query: str): 
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


