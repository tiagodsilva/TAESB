""" 
Methods to benchmark Celery's workers. 
""" 
# Sys 
import os 
import sys 
import glob 

# Celery 
from celery import Task 

# Benchmark 
import time 

class BenchmarkTask(task): 
    """ 
    A class to benchmark Celery's workers. 
    """ 
    # The boundaries of the execution time 
    _start_time: int = None 
    _current_time: int = None 
    
    def start_pipeline(self): 
        """ 
        Assert the start of the pipeline. 
        """ 
        self._start_time = time.time() # Persistent across processes 

    def timedelta(self): 
        """ 
        Compute the interval to execute the pipeline. 
        """ 
        self._current_time = time.time() 

        # Return the execution's interval 
        return self._current_time - self._start_time 

