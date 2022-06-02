""" 
Implement a list of callbacks for the Celery tasks. 
""" 
# Celery 
import celery 

# Docs 
from typing import List, Callable, Dict, Any 

class CallbacksList(object): 
    """ 
    A list of callbacks for the Celery application; it tackles the transfer of data. 
    """ 

    def __init__(self, 
            initialize: Callable[[Dict], Any], 
            callbacks: List[Callable[[Dict], Any]]): 
        """ 
        Constructor method for `CallbacksList`. 

        Parameters 
        --------- 
        args: celery.Task 
            each component of `args' is a task for celery, which uses an Map 
            as an parameter 
        """ 
        # List of the calbacks to be executed 
        self.initialize = initialize 
        self.callbacks = callbacks 
    
    def start(self, global_map: 'Map'): # Forward reference 
        """ 
        Execute callbacks on start of a simulation. 
        """ 
        json_map = global_map.to_json() 
        self.initialize.delay(json_map) 

    def stage_update(self, global_map: 'Map'): # Forward reference 
        """ 
        Execute each callback. 
        """ 
        # Serialize map 
        json_map = global_map.to_json() 
        for callback in self.callbacks: 
            callback.delay(json_map) 
