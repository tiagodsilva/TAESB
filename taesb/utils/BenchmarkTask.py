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
    
    _scenario_id: str = None 

    def start_pipeline(self, scenario_id: str): 
        """ 
        Assert the start of the pipeline. 
        """ 
        self._start_time = time.time() # Persistent across processes 
        
        # Intrudoce the scenario's identifier as an attribute 
        self._scenario_id = scenario_id 
    
    def timedelta(self, scenario_id: str): 
        """ 
        Compute the interval to execute the pipeline. 
        """ 
        self._current_time = time.time() 
        
        # Check the consistency of the scenario's identifier 
        self._scenario_id = scenario_id 

        # Return the execution's interval 
        return self._current_time - self._start_time 

