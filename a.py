import taesb 
from taesb.spark.SparkSubmit import ScheduleSpark 

if __name__ == "__main__": 
    spark = ScheduleSpark("taesb") 
    spark.schedule(stamp=5) 
