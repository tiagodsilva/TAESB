""" 
An async version of the map. 
""" 
# Import the map application 
from taesb.Map import Map 

class MapAsync(Map): 
    """ 
    Implement an asynchronous version of the map, compatible with the 
    Celery framework. 
    """ 

    def run_async(self, max_foods: int): 
        """ 
        Execute the simulation. 
        """ 
        super().run(max_foods=max_foods)

