""" 
Implement the Pheromone class.
"""

class Pheromone(object): 
    """ 
    A class characterizing a pheromone. 
    """ 

    lifetime: int # The pheromone's life time 
    init: int # The pheromone's born time 
    
    def __init__(self, lifetime: int, init: int): 
        """ 
        Constructor method for pheromone. 
        """ 
        self.lifetime = lifetime 
        self.init = init 

    def kill(self, iteration: int) -> bool: 
        """ 
        Whether to kill the pheromone at iteration `iteration`. 
        """ 
        return iteration > self.init + self.lifetime 
