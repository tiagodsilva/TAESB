""" 
Submit Spark's jobs (possibly scheduled). 
""" 
# Sys 
import os 
import sys 
import glob 

# IO 
import pyspark 
from pyspark.sql import SparkSession 
import pyspark.sql.functions as F 
from pyspark.conf import SparkConf 

import psycopg 
import time 

# Docs 
from typing import Dict, Any 

from .CelerySpark import update_stats 

# Start Spark session 
class ScheduleSpark(object):
    """ 
    Class to schedule Spark jobs. 
    """ 
    
    def __init__(self, app_name: str, 
                       spark_config: Dict[str, str], 
                       database_url: str, 
                       database_name: str, 
                       database_auth: Dict[str, str]): 
        """ 
        Constructor method for ScheduleSpark. 
        """ 
        # Instantiate attributes
        self.app_name = app_name 
        self.spark_config = spark_config 
        self.database_url = database_url 
        self.database_name = database_name 
        self.database_auth = database_auth 

        # and start a Spark session 
        self.spark_session = SparkSession \
                .builder 

        for config in spark_config: 
            # Update session's configuration 
            self.spark_session = self.spark_session \
                .config(config, spark_config[config]) 
        
        # Check if there is already a session 
        self.spark_session = self.spark_session \
                .getOrCreate() 
        
        # Iniitialize the data base 
        self.init_db() 

    def init_db(self): 
        """ 
        Initialize the data base. 
        """ 
        # Initialize the access to the data base 
        self.db_conn = pyscopg2.connect( 
                database=self.database_name, 
                user=self.database_auth["user"], 
                password=self.database_auth["password"] 
        ) 

    def execute_query(self, query: str): 
        """ 
        Execute a query to access the data base. 
        """ 
        cur = self.db_conn.cursor() 
        cur.execute(query) 
        cur.close() 
        self.db_conn.commit() 

    def read_table(self, tablename: str): 
        """ 
        Capture a table from the database at `database_url`. 
        """ 
        dataframe = self.spark_session.read \
                .format("jdbc") \
                .option("url", self.database_url) \
                .option("dbtable", tablename) \
                .option("user", self.database_auth["user"]) \
                .option("password", self.database_auth["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load() 

        # Return the data frame 
        return dataframe 

     def update_stats(self, 
            scenarios: int, 
            anthills: int, 
            ants_searching_food: int, 
            ants: int, 
            foods_in_anthills: int, 
            foods_in_deposit: int, 
            avg_execution_time: int, 
            fst_scenario_id: str, 
            fst_scenario_time: int, 
            slw_scenario_id: str, 
            slw_scenario_time: int, 
            avg_ant_food: float, 
            max_ant_food: int 
        ): 
        """ 
        Update the stats table in the database pointed by `db_conn`; 
        this is done periodically. 
        """ 
        # Write the query 
        query = """INSERT INTO stats 
        (stat_id, scenarios, anthills, ants_searching_food, ants, 
        foods_in_anthills, foods_in_deposit, avg_execution_time, 
        fst_scenario_id, fst_scenario_time, 
        slw_scenario_id, slw_scenario_time, 
        avg_ant_food, max_ant_food) 
VALUES 
        (1, {scenarios}, {anthills}, {ants_searching_food}, 
        {ants}, {foods_in_anthills}, {foods_in_deposit}, 
        {avg_execution_time}, '{fst_scenario_id}', {fst_scenario_time}, 
        '{slw_scenario_id}', {slw_scenario_time}, {avg_ant_food}, 
        {max_ant_food}) 
ON CONFLICT (stat_id) 
    DO 
        UPDATE SET scenarios = {scenarios}, 
                   anthills = {anthills}, 
                   ants_searching_food = {ants_searching_food}, 
                   ants = {ants}, 
                   foods_in_anthills = {foods_in_anthills}, 
                   foods_in_deposit = {foods_in_deposit}, 
                   avg_execution_time = {avg_execution_time}, 
                   fst_scenario_id = '{fst_scenario_id}', 
                   fst_scenario_time = {fst_scenario_time}, 
                   slw_scenario_id = '{slw_scenario_id}', 
                   slw_scenario_time = {slw_scenario_time}, 
                   avg_ant_food = {avg_ant_food}, 
                   max_ant_food = {max_ant_food};""".format( 
                           scenarios=scenarios,
                           anthills=anthills,
                           ants_searching_food=ants_searching_food,
                           ants=ants,
                           foods_in_anthills=foods_in_anthills,
                           foods_in_deposit=foods_in_deposit, 
                           avg_execution_time=avg_execution_time,
                           fst_scenario_id=fst_scenario_id,
                           fst_scenario_time=fst_scenario_time, 
                           slw_scenario_id=slw_scenario_id,
                           slw_scenario_time=slw_scenario_time,
                           avg_ant_food=avg_ant_food,
                           max_ant_food=max_ant_food 
                    ) 

        self.execute_query(query) 
    
    def schedule(self, 
            task: Callable[[Any], Any],
            stamp: str, 
            timeout: int=None): 
        """ 
        Schedule a job. 
        """ 
        start = time.time() 
        # Execute until timeout 
        while True: 
            if time.time() % start == stamp: 
                # Update the data base 
                task(self) # Should compute the quantities 
            
            # Check timeout 
            if timeout is not None and \
                    time.time() - start > timeout: 
                return 
        
     
