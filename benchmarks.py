""" 
Generate data for the benchmarks. 
""" 
# Sys 
import os 
import sys 

# Visualizations 
import taesb 
from taesb.vis import line_plot 
from taesb.spark.SparkSubmit import ScheduleSpark 

# Use Spark to compute the summaries 
import pyspark 
from pyspark.sql import functions as F 

def pipeline_time(table_name: str="benchmarks"): 
    """ 
    Compute the pipeline's execution times from the table `benchmarks`. 
    """ 
    # Instantiate a Spark session 
    spark = ScheduleSpark("taesb") 

    # Capture the `benchmarks` table 
    benchmarks = spark.read_table(table_name) 
    
    # Compute the temporal execution ranges
    ranges = benchmarks \
            .groupBy(["n_processes", "scenario_id"]) \
            .agg(
                    (F.max("computed_at").cast("long") - F.min("computed_at").cast("long")) \
                            .alias("execution_time")) 
    
    # Return the temporal execution range for each scenario 
    return ranges 
   
def generate_lineplots(ranges: pyspark.sql.DataFrame): 
    """ 
    Generate the lineplots for distinct quantity of processes. 
    """ 
    # Compute the average execution time for each process 
    execution_time = ranges \
            .groupBy("n_processes") \
            .agg(F.mean("execution_time").alias("execution_time")) \
            .toPandas() 

    # Write the visualizatiosn 
    line_plot(data=execution_time, 
            x="n_processes", 
            y="execution_time", 
            filename="benchmarks.png") 

if __name__ == "__main__": 
    ranges = pipeline_time(table_name="benchmarks") 
    generate_lineplots(ranges) 
    print(ranges) 

