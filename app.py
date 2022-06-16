""" 
Instantiate an application that gather the data from Postgres as a RDD with 
Spark. 
""" 
# Sys 
import pyspark 
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
import os 

# Benchmarks 
import time 

# Docs 
from typing import List 

# Instantiate Spark session 
spark = SparkSession \
        .builder \
        .appName("Ant Empire") \
        .config("spark.jars", "postgresql-42.3.6.jar") \
        .config("spark.master", "local[8]") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate() 

def read_table(tablename: str, database: str = "postgres"): 
    """ 
    Read a table from the database `adatabase`. 
    """ 
    rdd = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/{database}".format( 
                database=database)) \
            .option("dbtable", "{tablename}".format(tablename=tablename)) \
            .option("user", "tiago") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .load() 

    return rdd 

def query_db(query: str, database: str = "postgres"): 
    """ 
    Read a table correspondent to the query `query`. 
    """
    rdd = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/{database}".format( 
            database=database)) \
        .option("query", query) \
        .option("user", "tiago") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load() 
    
    # Return the resilient and distributed data set 
    return rdd 

def write_csvs(tablenames: List[str], 
        output_dir: str = "dummy_data"): 
    """ 
    Write CSVs for the tables in the PostgreSQL. 
    """ 
    # Check if output_dir exists 
    if not os.path.exists(output_dir): 
        os.mkdir(output_dir) 
    
    # Write the tables as CSVs 
    for tablename in tablenames: 
        # Identify table 
        rdd = read_table(tablename) 
        pandas_df = rdd.toPandas() 
        pandas_df.to_csv(os.path.join( 
            output_dir, tablename + ".csv" 
        )) 
    
if __name__ == "__main__": 
    write_csvs(["anthills", "ants", "scenarios", 
        "foods"]) 
    
    anthills = read_table("anthills") 
    ants = read_table("ants") 
    scenarios = read_table("scenarios") 
    stats = read_table("stats_local") 

    # Ants in active anthills 
    active_ants = ants.join( 
            anthills, 
            anthills.anthill_id == ants.anthill_id, 
            "inner" 
    ) 
    active_ants = active_ants.join( 
            scenarios, 
            scenarios.scenario_id == active_ants.scenario_id,
            "inner" 
    ) 
    active_ants = active_ants.filter( 
            active_ants.active == 1 
    ) 

    active_ants.show() 

    scenarios.show() 
    stats.show() 
