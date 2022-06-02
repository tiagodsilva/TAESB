""" 
Instantiate an application that gather the data from Postgres as a RDD with 
Spark. 
""" 
# Sys 
import pyspark 
from pyspark.sql import SparkSession 

# Benchmarks 
import time 

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

if __name__ == "__main__": 
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


