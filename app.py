""" 
Instantiate an application that gather the data from Postgres as a RDD with 
Spark. 
""" 
# Sys 
import pyspark 
from pyspark.sql import SparkSession 
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

    start = time.time() 
    rdd = read_table("scenarios") 
    elapsed = time.time() - start 
    print("Table:", elapsed) 

    # Compute the proportion of ants searching for foods 
    start = time.time() 
    # avg = rdd.agg({"searching_food": "avg"}) 
    elapsed = time.time() - start 
    print("Elasped", elapsed) 
    # print(avg.show()) 
    print(rdd.show()) 


