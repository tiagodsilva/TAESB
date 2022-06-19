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
import sched 
import json 

# Docs 
from typing import Dict, Any, List, Callable 
from types import SimpleNamespace 

# Benchmarks 
import time 

def benchmarks(message: str=None): 
    """ 
    Generate a decorator that displays benchmarks and messages `message`. 
    """ 
    def _benchmarks(func: Callable): 
        """ 
        Compute the benchmarks for the function `func`; it is used as a decorator.   
        """ 
        # Compute the benchmarks for the decorated function 
        def decorated(self, *args, **kwargs): 
            start = time.time()  
            data = func(self, *args, **kwargs) 
            current = time.time()  
            # Print a message with the benchmarks 
            print("[INFO]: {message}, {interval}s".format( 
                message=message, 
                interval= (current - start)
            )) 
            return data 
        # Return the decorated function 
        return decorated 

    # Return the decorator 
    return _benchmarks


# Start Spark session 
class ScheduleSpark(object):
    """ 
    Class to schedule Spark jobs. 
    """ 
    
    def __init__(self, 
            app_name: str, 
            database_name: str=os.environ["POSTGRESQL_DATABASE"], 
            database_host: str=os.environ["POSTGRESQL_HOST"], 
            database_user: str=os.environ["POSTGRESQL_USER"], 
            database_pwd: str=os.environ["POSTGRESQL_PASSWORD"]): 
        """ 
        Constructor method for ScheduleSpark. 
        """ 
        # Instantiate attributes
        self.app_name = app_name 
        self.database_name = database_name 
        self.database_host = database_host 
        self.database_user = database_user 
        self.database_pwd = database_pwd 
        # and start a Spark session 
        self.spark_session = SparkSession \
                .builder \
                .config("spark.jars", os.environ["SPARK_JARS"]) \
                .config("spark.ui.enabled", os.environ["SPARK_UI_ENABLED"]) \
                .getOrCreate() 
        
        # Update log level 
        sc = self.spark_session.sparkContext 
        sc.setLogLevel("ERROR") 

        # Iniitialize the data base 
        # SparkSQL is not absolutely compatible with the queries 
        # we should execute 
        self.init_db() 

    def init_db(self): 
        """ 
        Initialize the data base. 
        """ 
        # Initialize the access to the data base 
        self.db_conn = psycopg2.connect( 
                host=self.database_host, 
                database=self.database_name, 
                user=self.database_user, 
                password=self.database_pwd 
        ) 

    def execute_query(self, query: str): 
        """ 
        Execute a query to access the data base. 
        """ 
        cur = self.db_conn.cursor() 
        cur.execute(query) 
        cur.close() 
        self.db_conn.commit() 
    
    def read_table(self, tablename: str, 
            spark_conn: SparkSession): 
        """ 
        Capture a table from the database at `database_url`. 
        """ 
        dataframe = spark_conn.option("dbtable", tablename) \
                .load() 

        # Return the data frame 
        return dataframe 

    @benchmarks(message="Fetch data from the database") 
    def read_tables(self, tablenames: List[str]): 
        """ 
        Capture the tables at `tablenames` and compute a dictionary for them. 
        """ 
        # A dictionary to combine the tables 
        tables = dict() 

        # Use Spark API to identify the database 
        spark_conn = self.spark_session \
                .read \
                .format("jdbc") \
                .option("url", "{driver}://{host}/{database}".format( 
                    driver="jdbc:postgresql", 
                    host=self.database_host, 
                    database=self.database_name) 
                ) \
                .option("user", self.database_user) \
                .option("password", self.database_pwd) \
                .option("driver", "org.postgresql.Driver") 

        # Iterate across the names of the tables 
        for tablename in tablenames: 
            # Fetch the table from the database 
            tables[tablename] = self.read_table(tablename, 
                    spark_conn=spark_conn) 
        # Return the tables 
        return tables 

    @benchmarks(message="Insert global quantities in the database") 
    def _update_global(self, 
            data: SimpleNamespace): 
        """ 
        Update the stats table in the database pointed by `db_conn`; 
        this is done periodically. 

        Parameters: 
        ---------- 
        data: SimpleNamespace 
            The data that will be inserted in the database; the attributes are 
                n_scenarios 
                n_anthills 
                n_ants_searching_food 
                n_ants 
                foods_in_anthills 
                foods_in_deposit 
                foods_in_transit 
                foods_total 
                avg_execution_time 
                fst_scenario_id 
                fst_scenario_time 
                slw_scenario_id 
                slw_scenario_time 
                avg_ant_food 
                max_ant_food 
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
                           n_scenarios=data.n_scenarios,
                           n_anthills=data.n_anthills,
                           n_ants_searching_food=data.n_ants_searching_food,
                           n_ants=data.n_ants,
                           foods_in_anthills=data.foods_in_anthills,
                           foods_in_deposit=data.foods_in_deposit, 
                           foods_in_transit=data.foods_in_transit, 
                           foods_total=data.foods_total, 
                           avg_execution_time=data.avg_execution_time,
                           fst_scenario_id=data.fst_scenario_id,
                           fst_scenario_time=data.fst_scenario_time, 
                           slw_scenario_id=data.slw_scenario_id,
                           slw_scenario_time=data.slw_scenario_time,
                           avg_ant_food=data.avg_ant_food,
                           max_ant_food=data.max_ant_food 
                    ) 

        self.execute_query(query) 
    
    @benchmarks(message="Insert the local quantities in the database") 
    def _update_local(self, 
            data: SimpleNamespace): 
        """ 
        Update the table that displays local statistics. 
        
        Parameters 
        ----------
        data: SimpleNamespace 
            The attributes that will be modified in the database; specifically, 
            each value in this namespace is a list of N quantities, in which N equals 
            the quantity of instances inserted in the database. Hence, the values are 
                scenario_id_l: List[str] 
                n_anthills_l: List[int] 
                n_foods_l: List[int] 
                execution_time_l: List[int] 
                active_l: List[int] 
        """ 
        query = """INSERT INTO stats_local
        (scenario_id, 
        n_anthills, 
        n_ants, 
        n_foods, 
        execution_time, 
        active 
)""" 
        
        # Consolidate the queries in a list 
        queries = list() 
        for (scenario_id, n_anthills, n_ants, n_foods, execution_time, active) in \
                zip(data.scenario_id_l, 
                    data.n_anthills_l, 
                    data.n_ants_l, 
                    data.n_foods_l, 
                    data.execution_time_l, 
                    data.active_l): 
            # Insert the instance in the data base; if the scenario already 
            # exists, update it 
            scenario_query = query + """
VALUES 
            ('{scenario_id}', 
            {n_anthills}, 
            {n_ants}, 
            {n_foods}, 
            {execution_time}, 
            {active}) 
ON CONFLICT (scenario_id) 
    DO 
        UPDATE SET n_anthills = {n_anthills}, 
                   n_ants = {n_ants}, 
                   n_foods = {n_foods}, 
                   execution_time = {execution_time}, 
                   active = {active};""".format(scenario_id=scenario_id, 
                           n_anthills=n_anthills, 
                           n_ants=n_ants,
                           n_foods=n_foods, 
                           execution_time=execution_time, 
                           active=active
                    ) 
        
            # Append the current query to the list of queries 
            queries.append(scenario_query) 

        # Execute the queries jointly 
        self.execute_query("\n".join(queries)) 
    
    @benchmarks("Insert the atomic quantities in the database") 
    def _update_atomic(self, 
            data: SimpleNamespace): 
        """ 
        Update the data with atomic values, for each anthill. 

        Parameters 
        --------- 
        data: SimpleNamespace 
            The attributes that will be modified in the database, in an format 
            equivalent to that in `_update_local`; the values of this namespace contemplate 
                scenario_id_l: List[str] 
                anthill_id_l: List[str] 
                n_ants_l: List[int] 
                n_ants_searching_food_l: List[int] 
                foods_in_anthills_l: List[int] 
                foods_in_transit_l: List[int] 
                probability_l: List[float] 
        """ 
        query = """INSERT INTO stats_atomic 
        (scenario_id, 
        anthill_id, 
        n_ants, 
        n_ants_searching_food, 
        foods_in_anthills, 
        foods_in_transit, 
        probability)""" 
    
        # Consolidate the queries in a list 
        queries = list() 
        
        # Insert a query for each instance 
        for scenario_id, anthill_id, n_ants, n_ants_searching_food, \
                foods_in_anthills, foods_in_transit, probability in \
                zip(data.scenario_id_l, 
                    data.anthill_id_l, 
                    data.n_ants_l, 
                    data.n_ants_searching_food_l, 
                    data.foods_in_anthills_l, 
                    data.foods_in_transit_l, 
                    data.probability_l): 

            # Write the query
            anthill_query = query + """
    VALUES 
            ('{scenario_id}', 
            '{anthill_id}', 
            {n_ants}, 
            {n_ants_searching_food}, 
            {foods_in_anthills}, 
            {foods_in_transit}, 
            {probability}  
    ) 
    ON CONFLICT (scenario_id, anthill_id) 
        DO
            UPDATE SET n_ants = {n_ants}, 
                       n_ants_searching_food = {n_ants_searching_food}, 
                       foods_in_anthills = {foods_in_anthills}, 
                       foods_in_transit = {foods_in_transit}, 
                       probability = {probability};""".format( 
                               scenario_id=scenario_id,
                               anthill_id=anthill_id, 
                               n_ants=n_ants,
                               n_ants_searching_food=n_ants_searching_food,
                               foods_in_anthills=foods_in_anthills,
                               foods_in_transit=foods_in_transit,
                               probability=probability
                        ) 
            
            # Append the query to the list 
            queries.append(anthill_query) 
        # Execute the DML queries 
        self.execute_query("\n".join(queries)) 

    def update_stats(self):
        """
        Update the appropriate data in the analytical database.
        """
        # print("Update the database, Luke!")
        print("[INFO]: Execute query") 
        # Read the tables
        ants_tn = "ants" # the suffix `tn` stands for table name  
        anthills_tn = "anthills" 
        foods_tn = "foods" 
        scenarios_tn = "scenarios" 
        tables_names = [ants_tn, anthills_tn, foods_tn, scenarios_tn]
        
        # And instantiate a container for the data frames
        tables = self.read_tables(tables_names) 

        # print(tables)
        # Compute the desired quantities 
        try: 
            global_stats = self.compute_global_stats(tables,
                    ants_tn=ants_tn,
                    anthills_tn=anthills_tn, 
                    foods_tn=foods_tn, 
                    scenarios_tn=scenarios_tn) 
            local_stats = self.compute_local_stats(tables, 
                    ants_tn=ants_tn, 
                    anthills_tn=anthills_tn, 
                    foods_tn=foods_tn, 
                    scenarios_tn=scenarios_tn) 
            atomic_stats = self.compute_atomic_stats(tables, 
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

        self._update_global(global_stats) 
        self._update_local(local_stats) 
        self._update_atomic(atomic_stats) 

    @benchmarks(message="Compute the global quantities") 
    def compute_global_stats(self, 
            tables: Dict[str, pyspark.sql.DataFrame], 
            ants_tn: str, 
            anthills_tn: str, 
            foods_tn: str, 
            scenarios_tn: str): 
        """ 
        Compute the quantities and generate the values, which . 
        """ 
        # Identifier for these quantities; this is appropriate for joining 
        # values from distinct columns, and it is moreover a surrogate for a 
        # suitable dimensional model 
        stat_id = 1 

        # We implement a bottom-up algorithm, in which we start 
        # with the most granular table and succeedingly increment 
        # choose other tables 
        data = tables[ants_tn] \
                .agg(F.max("captured_food").alias("max_ant_food"), 
                    F.mean("captured_food").alias("avg_ant_food"), 
                    F.sum("searching_food").alias("n_ants_searching_food"), 
                    F.count("ant_id").alias("n_ants") 
                ) 
        # Insert column with fixed value for subsequent joins 
        data = data.withColumn("stat_id", F.lit(stat_id)) 
        
        # Compute the data regarding the foods 
        foods_in_deposit = tables[foods_tn] \
            .agg(F.sum("current_volume").alias("foods_in_deposit")) \
            .withColumn("stat_id", F.lit(stat_id))  
        
        data = data.join( 
                foods_in_deposit, 
                on="stat_id",
                how="inner" 
        ) 
        # Compute the data regarding the anthills 
        anthills_data = tables[anthills_tn] \
                .agg(F.count("anthill_id").alias("n_anthills"), 
                    F.sum("food_storage").alias("foods_in_anthills")) \
                .withColumn("stat_id", F.lit(stat_id)) 
        
        data = data.join( 
                anthills_data, 
                on="stat_id", 
                how="inner" 
        ) 

        scenarios_data = tables[scenarios_tn] \
                .agg(F.count("scenario_id").alias("n_scenarios"), 
                    F.max("execution_time").alias("slw_scenario_time"), 
                    F.min("execution_time").alias("fst_scenario_time"), 
                    F.mean("execution_time").alias("avg_execution_time")) \
                .withColumn("stat_id", F.lit(stat_id)) 

        # Join the data 
        data = data.join( 
                scenarios_data, 
                on="stat_id", 
                how="inner" 
        ) 
        
        # Compute the total quantity of foods; a pretty ad hoc procedure 
        data = data \
                .withColumn("foods_in_transit", F.col("n_ants") - F.col("n_ants_searching_food")) 

        data = data \
                .withColumn("foods_total", F.col("foods_in_anthills") + \
                        F.col("foods_in_transit") + \
                        F.col("foods_in_deposit") 
                ) 
        
        # Compute the boundary scenarios 
        fst_scenario = tables[scenarios_tn].join( 
                data, 
                data.fst_scenario_time == tables[scenarios_tn].execution_time, 
                "left_anti"
        ).selectExpr("scenario_id AS fst_scenario_id") \
                .limit(1) \
                .withColumn("stat_id", F.lit(stat_id)) 

        slw_scenario = tables[scenarios_tn].join( 
                data, 
                data.slw_scenario_time == tables[scenarios_tn].execution_time, 
                "left_anti" 
        ).selectExpr("scenario_id AS slw_scenario_id") \
                .limit(1) \
                .withColumn("stat_id", F.lit(stat_id)) 

        # Join the data 
        data = data.join( 
                fst_scenario, 
                on="stat_id", 
                how="inner" 
        ) 

        data = data.join( 
                slw_scenario, 
                on="stat_id", 
                how="inner" 
        ) 
        
        # Convert data to dict 
        data = data.toPandas().to_dict() 
        # Return the data 
        return SimpleNamespace(**data)  

    @benchmarks(message="Compute the local quantities") 
    def compute_local_stats(self, 
            tables: pyspark.sql.DataFrame, 
            scenarios_tn: str, 
            ants_tn: str, 
            anthills_tn: str, 
            foods_tn: str): 
        """ 
        Compute the quantities for the local, per scenario, data.
        """ 
        # Join the anthills and the scenarios tables
        joint_anthills = tables[scenarios_tn].join( 
                tables[anthills_tn], 
                on="scenario_id", 
                how="inner" 
            ) 
        # Consolidate the data in a JSON 
        data = dict() 

        # Group the tables by scenarios 
        group_by_scenario = joint_anthills \
                .groupBy("scenario_id") 

        # Compute the quantity of foods in each anthill, 
        # which equals the sum 
        # `food_in_anthills` + `foods_in_transit` + `foods_in_deposit` 
        
        # Compute foods in anthills 
        foods_in_anthills = group_by_scenario \
                .agg(F.sum("food_storage").alias("foods_in_anthills")) 

        # Compute foods in transit 
        # For this, we should compute the ants in the current scenario 
        joint_ants = tables[scenarios_tn].join( 
                tables[ants_tn].join(
                    tables[anthills_tn], 
                    on="anthill_id", 
                    how="inner" 
                ), 
                on="scenario_id", 
                how="inner" 
        ) 

        # Sum the quantity of ants searching food per scenario; this 
        # equals the quantity of foods in transit 
        foods_in_transit = joint_ants \
                .groupBy("scenario_id") \
                .agg(F.sum("searching_food").alias("foods_in_transit")) 

        # Compute the quantity of foods in deposit, 
        # which is available in the `foods` table 
        foods_in_deposit = tables[foods_tn] \
                .groupBy("scenario_id") \
                .agg(F.sum("current_volume").alias("foods_in_deposit")) 

        # Join the tables with the quantities of foods 
        foods = foods_in_anthills.join( 
                foods_in_transit, 
                on="scenario_id", 
                how="inner" 
        ) 

        foods = foods.join( 
                foods_in_deposit, 
                on="scenario_id", 
                how="inner" 
        ) 
        
        # Check https://stackoverflow.com/questions/44502095/
        from operator import add 
        from functools import reduce 
        foods = foods \
                .select(
                        ["scenario_id", 
                        (F.col("foods_in_anthills") + F.col("foods_in_deposit") + \
                                F.col("foods_in_transit")).alias("n_foods") 
                        ] 
                ) 
       
        # Compute the quantity of anthills per scenario 
        anthills = group_by_scenario \
                .agg(F.count("anthill_id").alias("n_anthills")) 

        # Compute the quantity of ants per scenario 
        ants = group_by_scenario \
                .agg(F.sum("total_ants").alias("n_ants")) 

        # Join the anthills, ants and foods tables with the scenarios table 
        scenarios = tables[scenarios_tn].join( 
                foods, 
                on="scenario_id", 
                how="inner" 
        ) 

        scenarios = scenarios.join( 
                anthills, 
                on="scenario_id", 
                how="inner" 
        ) 

        scenarios = scenarios.join( 
                ants, 
                on="scenario_id", 
                how="inner" 
        ) 

        data = scenarios \
                .select(["scenario_id", "n_ants", "n_foods", 
                    "n_anthills", "execution_time", "active"]) \
                .toPandas() \
                .to_dict("list") 
        
        # Insert a suffix to the data's columns 
        data = {key+"_l":data[key] for key in data} 

        # Return the aggregated data 
        return SimpleNamespace(**data) 
    
    @benchmarks(message="Compute atomic quantities") 
    def compute_atomic_stats(self, 
            tables: Dict[str, pyspark.sql.DataFrame], 
            ants_tn: str,
            anthills_tn: str, 
            foods_tn: str, 
            scenarios_tn: str): 
        """ 
        Compute the atomic quantities (per anthill, per scenario). 
        """ 
        # We should identify, for each anthill in each scenario, 
        #   + the quantity of ants, and the percentual of those searching food, 
        #   + the quantity of food stored, and the percentual in transit, and 
        #   + the probability of winning the game, which is proportional 
        #       to the quantity of food stored 
        
        # Instantiate a object to write the data 
        data = dict() 
        
        # Write aimed fields 
        n_ants = "n_ants" 
        n_ants_searching_food = "n_ants_searching_food" 
        foods_in_anthills = "foods_in_anthills" 
        foods_in_transit = "foods_in_transit" 
        probability = "probability" 

        # Join the ants table and the anthills table; group by anthill_id 
        joint_ants = tables[ants_tn].join( 
                tables[anthills_tn], 
                on="anthill_id", 
                how="inner" 
        ).groupBy("anthill_id") 

        # Compute the quantity of ants 
        data[n_ants] = joint_ants \
                .agg(F.count("ant_id").alias(n_ants))  

        # and the quantity of ants searching food 
        data[n_ants_searching_food] = joint_ants \
                .agg(F.sum("searching_food").alias(n_ants_searching_food))  

        # Compute the quantity of foods in the deposit 
        data[foods_in_anthills] = tables[anthills_tn] \
                .selectExpr("anthill_id", "food_storage AS {foods_in_anthills}".format( 
                    foods_in_anthills=foods_in_anthills))  

        # Compute the quantity of foods in transit 
        data[foods_in_transit] = tables[ants_tn] \
                .groupBy("anthill_id") \
                .agg(F.sum(1 - F.col("searching_food")).alias(foods_in_transit)) 

        # Compute the probability of winning, which is proportional to 
        # `foods_in_deposit` 
        total_foods_in_deposit = tables[anthills_tn] \
                .groupBy("scenario_id") \
                .agg(F.sum(F.col("food_storage") + 1).alias("total_foods")) 

        data[probability] = tables[anthills_tn].join( 
                total_foods_in_deposit, 
                on="scenario_id", 
                how="inner"
        ).select(["scenario_id", "anthill_id", "food_storage", "total_foods"]) \
                .withColumn(probability, (F.col("food_storage") + 1)/F.col("total_foods")) 

        # Join the tables 
        datatb = data[n_ants] 
        # Release object 
        del data[n_ants] 
        for field in [n_ants_searching_food, foods_in_anthills, \
                foods_in_transit, probability]: 
            # Join the tables 
            datatb = datatb.join( 
                    data[field], 
                    on="anthill_id",
                    how="inner" 
            ) 
            # Release object 
            del data[field] 
         
        # Release object 
        del data 

        # Convert the data to a JSON 
        datatb = datatb \
                .select([
                    "anthill_id", 
                    "scenario_id", 
                    foods_in_anthills, 
                    foods_in_transit, 
                    probability, 
                    n_ants, 
                    n_ants_searching_food]) \
                .toPandas().to_dict("list") 
        
        # Update suffxes (for consistency with subsequent procedures) 
        data = dict() 
        for key in datatb: 
            # Update column's name 
            data[key + "_l"] = datatb[key] 
        
        # Release object 
        del datatb  

        # Return the current data 
        return SimpleNamespace(**data) 
    
    def schedule(self, # Local execution  
            stamp: str, 
            timeout: int=None): 
        """ 
        Schedule a job. 
        """ 
        # Intantiate scheduler 
        scheduler = sched.scheduler(time.time, time.sleep) 
    
        start = time.time() 
        while True: 
            # Execute the scheduler 
            try: 
                scheduler.enter( 
                        stamp, 
                        priority=1, 
                        action=self.update_stats 
                )
                scheduler.run() 
            except Exception as err: 
                # Does not interrupt the pipeline on exceptions 
                print("[ERROR]: {err}, skipping current iteration".format(err=str(err))) 
                continue 
            
            # Check the execution length  
            if timeout is not None and time.time() - start > timeout: 
                return 

if __name__ == "__main__": 
    # Capture Spark's configurations 
    # Instantiate a session for Spark 
    spark = ScheduleSpark("taesb", 
            database_name=os.environ["POSTGRESQL_DATABASE"], 
            database_host=os.environ["POSTGRESQL_HOST"], 
            database_user=os.environ["POSTGRESQL_USER"], 
            database_pwd=os.environ["POSTGRESQL_PASSWORD"]) 
    spark.schedule(stamp=5) 

