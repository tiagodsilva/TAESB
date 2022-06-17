""" 
Initiate periodic Spark jobs. 
""" 
from taesb.utils.SparkSubmit import ScheduleSpark 

# Interval between a pair of executions 
STAMP = 5 
APP_NAME = "taesb" 

if __name__ == "__main__": 
    spark = ScheduleSpark(APP_NAME) 
    # Execute periodically 
    spark.schedule(STAMP) 
