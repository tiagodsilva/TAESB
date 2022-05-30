""" 
Implement a list of callbacks for the Celery tasks. 
""" 
# Celery 
import celery 

class CallbacksList(object): 
    """ 
    A list of callbacks for the Celery application; it tackles the transfer of data. 
    """ 

    def __init__(self, *args): 
        """ 
        Constructor method for `CallbacksList`. 

        Parameters 
        --------- 
        args: celery.Task 
            each component of `args' is a task for celery, which uses an Map 
            as an parameter 
        """ 
        # List of the calbacks to be executed 
        self.callbacks = list(args) 

    def execute(self, global_map: 'Map'): # Forward reference 
        """ 
        Execute each callback. 
        """ 
        # Serialize map 
        json_map = global_map.to_json() 
        for callback in self.callbacks: 
            callback.delay(json_map) 
