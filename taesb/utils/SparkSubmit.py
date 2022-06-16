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

import psycopg2 
import time 
import json 

# Docs 
from typing import Dict, Any, List

# Configurations for Spark and Postgres 
SPARK_CONFIG = "spark_config.json" 
DB_AUTH = "postgres_auth.json" 

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
        self.db_conn = psycopg2.connect( 
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

    def _update_global(self, 
            n_scenarios: int, 
            n_anthills: int, 
            n_ants_searching_food: int, 
            n_ants: int, 
            foods_in_anthills: int, 
            foods_in_deposit: int, 
            foods_in_transit: int, 
            foods_total: int, 
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
        query = """INSERT INTO stats_global
        (stat_id, 
        n_scenarios, 
        n_anthills, 
        n_ants_searching_food, 
        n_ants, 
        foods_in_anthills, 
        foods_in_deposit, 
        foods_in_transit, 
        foods_total, 
        avg_execution_time, 
        fst_scenario_id, 
        fst_scenario_time, 
        slw_scenario_id, 
        slw_scenario_time, 
        avg_ant_food, 
        max_ant_food) 
VALUES 
        (1, 
        {n_scenarios}, 
        {n_anthills}, 
        {n_ants_searching_food}, 
        {n_ants}, 
        {foods_in_anthills}, 
        {foods_in_deposit}, 
        {foods_in_transit}, 
        {foods_total}, 
        {avg_execution_time}, 
        '{fst_scenario_id}', 
        {fst_scenario_time}, 
        '{slw_scenario_id}', 
        {slw_scenario_time}, 
        {avg_ant_food}, 
        {max_ant_food}) 
ON CONFLICT (stat_id) 
    DO 
        UPDATE SET n_scenarios = {n_scenarios}, 
                   n_anthills = {n_anthills}, 
                   n_ants_searching_food = {n_ants_searching_food}, 
                   n_ants = {n_ants}, 
                   foods_in_anthills = {foods_in_anthills}, 
                   foods_in_deposit = {foods_in_deposit}, 
                   foods_in_transit = {foods_in_transit}, 
                   foods_total = {foods_total}, 
                   avg_execution_time = {avg_execution_time}, 
                   fst_scenario_id = '{fst_scenario_id}', 
                   fst_scenario_time = {fst_scenario_time}, 
                   slw_scenario_id = '{slw_scenario_id}', 
                   slw_scenario_time = {slw_scenario_time}, 
                   avg_ant_food = {avg_ant_food}, 
                   max_ant_food = {max_ant_food};""".format( 
                           n_scenarios=n_scenarios,
                           n_anthills=n_anthills,
                           n_ants_searching_food=n_ants_searching_food,
                           n_ants=n_ants,
                           foods_in_anthills=foods_in_anthills,
                           foods_in_deposit=foods_in_deposit, 
                           foods_in_transit=foods_in_transit, 
                           foods_total=foods_total, 
                           avg_execution_time=avg_execution_time,
                           fst_scenario_id=fst_scenario_id,
                           fst_scenario_time=fst_scenario_time, 
                           slw_scenario_id=slw_scenario_id,
                           slw_scenario_time=slw_scenario_time,
                           avg_ant_food=avg_ant_food,
                           max_ant_food=max_ant_food 
                    ) 

        self.execute_query(query) 
    
    def _update_local(self, 
            scenario_id_l: List[str], 
            n_anthills_l: List[int], 
            n_foods_l: List[int], 
            excecution_time_l: List[int] 
        ): 
        """ 
        Update the table that displays local statistics. 
        """ 
        query = """INSERT INTO stats_local
        (scenario_id, 
        n_anthills, 
        n_ants, 
        n_foods, 
        execution_time
)""" 
        
        # Consolidate the queries in a list 
        queries = list() 
        for (scenario_id, n_anthills, n_foods, execution_time) in \
                zip(scenario_id_l, n_anthills_l, n_foods_l, execution_time_l): 
            # Insert the instance in the data base; if the scenario already 
            # exists, update it 
            scenario_query = query + """VALUES 
            ({scenario_id}, 
            {n_anthills}, 
            {n_ants}, 
            {n_foods}, 
            {execution_time}) 
ON CONFLICT (scenario_id) 
    DO 
        UPDATE SET n_anthills = {n_anthills}, 
                   n_ants = {n_ants}, 
                   n_foods = {n_foods}, 
                   execution_time = {execution_time};""".format( 
                           n_anthills=n_anthills, 
                           n_ants=n_ants,
                           n_foods=n_foods, 
                           execution_time=execution_time 
                    ) 
        
            # Append the current query to the list of queries 
            queries.append(scenario_query) 

        # Execute the queries jointly 
        self.execute_query("\n".join(queries)) 
        
    def update_stats(self):
        """
        Update the appropriate data in the analytical database.
        """
        # print("Update the database, Luke!")
        # Read the tables
        ants_tn = "ants" # the suffix `tn` stands for table name  
        anthills_tn = "anthills" 
        foods_tn = "foods" 
        scenarios_tn = "scenarios" 
        tables_names = [ants_tn, anthills_tn, foods_tn, scenarios_tn]

        # And instantiate a container for the data frames
        tables = {
            dbtable:None for dbtable in tables_names
        }

        # Iterate across the tables
        for table in tables:
            tables[table] = self.read_table(table)

        # print(tables)

        # Compute the desired statistics
        # We send the name of the tables to guarantee 
        # that these quantities are not reassigned everywhere 
        try: 
            curr_stats = self.compute_global_stats(tables,
                    ants_tn=ants_tn,
                    anthills_tn=anthills_tn, 
                    foods_tn=foods_tn, 
                    scenarios_tn=scenarios_tn) 
        except IndexError as err: 
            print("[ERROR]: {err}".format(err=err))
            # There are no instances in the table 
            return  
        except TypeError as err: 
            print("[ERROR]: {err}".format(err=err)) 
            # sum `NoneType` with an integer 
            return 

        self._update_global(**curr_stats) 

    def compute_global_stats(self, 
            tables: Dict[str, pyspark.sql.DataFrame], 
            ants_tn: str, 
            anthills_tn: str, 
            foods_tn: str, 
            scenarios_tn: str): 
        """ 
        Compute the quantities and generate the values, which . 
        """ 
        # Generate a table to gather the results 
        data = dict() 

        # Compute the quantity of scenarios 
        data["n_scenarios"] = tables[scenarios_tn].count() 
        # and the quantity of anthills 
        data["n_anthills"] = tables[anthills_tn].count() 
        
        # and the quantity of foods in deposit 
        data["foods_in_deposit"] = tables[foods_tn] \
                .agg(F.sum("current_volume")) \
                .collect()[0][0] 

        # Quantity of ants alive 
        data["n_ants"] = tables[ants_tn].count() 
        # and quantity searching food 
        data["n_ants_searching_food"] = tables[ants_tn] \
                .agg(F.sum("searching_food")) \
                .collect()[0][0] 

        # Quantity of foods in transit 
        data["foods_in_transit"] = data["n_ants"] - data["n_ants_searching_food"] 

        # Quantity of foods in the anthills 
        data["foods_in_anthills"] = tables[anthills_tn] \
                .agg(F.sum("food_storage")) \
                .collect()[0][0] 

        # Quantity of foods in total 
        data["foods_total"] = data["foods_in_deposit"] + data["foods_in_transit"] + \
                data["foods_in_anthills"] 
        
        # Capture inactive scenarios 
        inactive_scenarios = tables[scenarios_tn] \
                .filter(tables[scenarios_tn].active != 1) 

        # Compute the average execution time within the inactive scenarios 
        data["avg_execution_time"] = inactive_scenarios.agg(F.mean("execution_time")) \
                .collect()[0][0] 

        # Execution time in the scenarios 
        ord_scenarios = inactive_scenarios \
                .orderBy(F.desc("execution_time")) 

        fst_scenario = ord_scenarios \
                .take(1)[0] \
                .asDict() 
        slw_scenario = ord_scenarios \
                .tail(1)[0] \
                .asDict() 

        # Identify the ID and the execution time for these scenarios 
        data["fst_scenario_id"] = fst_scenario["scenario_id"] 
        data["fst_scenario_time"] = fst_scenario["execution_time"] 
        data["slw_scenario_id"] = slw_scenario["scenario_id"] 
        data["slw_scenario_time"] = slw_scenario["execution_time"] 

        # Check the ants that captured foods 
        ants_foods = tables[ants_tn] \
                .agg(F.avg("captured_food"), F.max("captured_food")) \
                .collect()[0] 

        data["avg_ant_food"], data["max_ant_food"] = ants_foods 
        
        # Return the data 
        return data 
    
    def schedule(self, 
            stamp: str, 
            timeout: int=None): 
        """ 
        Schedule a job. 
        """ 
        start = time.time() 
        # Execute until timeout 
        while True: 
            if (time.time() - start) %  stamp ==  0: 
                print("[INFO]: Execute query") 
                # Update the data base 
                self.update_stats() # Should compute the quantities 
            
            # Check timeout 
            if timeout is not None and \
                    time.time() - start > timeout: 
                return 
       
if __name__ == "__main__": 
    # Capture Spark's configurations 
    with open(SPARK_CONFIG, "r") as stream: 
        spark_config = json.load(stream) 
    
    # And the database authentication tab 
    with open(DB_AUTH, "r") as stream: 
        database_auth = json.load(stream) 

    database_name = "postgres" 
    database_url = "jdbc:postgresql://localhost:5432/{database}" \
            .format(database=database_name) 
        
    # Instantiate a session for Spark 
    spark = ScheduleSpark("taesb", 
            spark_config, 
            database_url, 
            database_name, 
            database_auth 
    ) 
    
    spark.schedule(stamp=5) 
   
