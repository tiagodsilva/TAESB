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

import psycopg2 

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

        # Get (or create) the current Spark session 
        self.spark_session = SparkSession \
                .builder \
                .appName(self.app_name) \
                .config("spark.jars", "postgresql-42.3.6.jar") \
                .getOrCreate() 

        # Update spark's logging 
        sc = pyspark.SparkContext.getOrCreate() 
        sc.setLogLevel("FATAL") 

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
    
    def read_table(self, tablename: str): 
        """ 
        Capture a data table from the database. 
        """ 
        # Query a table in the database 
        rdd = self.spark_session \
                .read \
                .format("jdbc") \
                .option("url", self.db_url) \
                .option("dbtable", tablename) \
                .option("user", "tiago") \
                .option("password", "password") \
                .option("driver", "org.postgresql.Driver") \
                .load() 

        # Return the resilient and distributed data set
        return rdd 
    
    def query_db(self, query: str): 
        """ 
        Capture a registers with a query in the database. 
        """ 
        rdd = self.spark_session \
                .read \
                .format("jdbc") \
                .option("url", self.db_url) \
                .optoin("query", "query") \
                .option("user", "tiago") \
                .option("password", "password") \
                .option("driver", "org.postgresql.Driver") \
                .load() 

        # Return the resilient and distributed data set 
        return rdd 

