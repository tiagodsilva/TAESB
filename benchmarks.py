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

BENCHMARKS_FOLDER = "benchmarks" 

def pipeline_time(table_name: str="benchmarks"): 
    """ 
    Compute the pipeline's execution times from the table `benchmarks`. 
    """ 
    # Instantiate a Spark session 
    spark = ScheduleSpark("taesb") 

    # Capture the `benchmarks` table 
    benchmarks = spark.read_tables([table_name]) 
    
    # Compute the temporal execution ranges
    ranges = benchmarks[table_name] \
            .groupBy(["n_processes", "scenario_id"]) \
            .agg(
                    (F.max("computed_at").cast("long") - F.min("computed_at").cast("long")) \
                            .alias("execution_time")) 
    
    # Return the temporal execution range for each scenario 
    return ranges 
   
def generate_lineplots(ranges: pyspark.sql.DataFrame, 
        data_filename: str, 
        figure_filename: str): 
    """ 
    Generate the lineplots for distinct quantity of processes. 
    """ 
    # Compute the average execution time for each process 
    execution_time = ranges \
            .groupBy("n_processes") \
            .agg(F.mean("execution_time").alias("execution_time")) \
            .toPandas() 
        
    # Write the benchmarks to the disk 
    ranges.agg(F.mean("execution_time").alias("execution_time")) \
            .toPandas() \
            .to_csv(data_filename, index=None) 

    # Write the visualizatiosn 
    line_plot(data=execution_time, 
            x="n_processes", 
            y="execution_time", 
            x_label="Quantidade de processos", 
            y_label="Tempo de execução", 
            filename=figure_filename) 

if __name__ == "__main__": 
    args = sys.argv 

    # Assert folder's existence 
    if not os.path.exists(BENCHMARKS_FOLDER): 
        os.mkdir(BENCHMARKS_FOLDER) 

    if len(args) <= 1: 
        print("Use\npython benchmarks.py [instances]") 
        exit(1) 
    
    figure_filename = os.path.join(BENCHMARKS_FOLDER, "benchmarks{instances}.png".format( 
        instances=args[1])) 
    data_filename = os.path.join(BENCHMARKS_FOLDER, "benchmarks{instances}.csv".format( 
        instances=args[1])) 

    ranges = pipeline_time(table_name="benchmarks") 
    generate_lineplots(ranges, 
            data_filename=data_filename, 
            figure_filename=figure_filename) 
    print(ranges) 
