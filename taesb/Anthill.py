""" 
Implement the Anthill class. 
"""

class Anthill(object): 
    """ 
    An anthill, which captures ants. 
    """ 

    name: str # The anthill's name 
    x_pos: int # Its horizontal location 
    y_pos: int # Its vertical location 
    initial_ants: int # The initial quanity of ants 
    
    def __init__(self, name: str, 
            x_pos: int, 
            y_pos: int, 
            initial_ants: int): 
        """ 
        Constructor method for an Anthill with name `name` and at location (`x_pos`, `y_pos`). 
        The initial quantity of ants equals `initial_ants`. 
        """ 
        self.x_pos = x_pos 
        self.y_pos = y_pos 
        self.initial_ants = initial_ants 
        self.name = name 

        self.food_storage = 0 

    def increment_food(self): 
        """ 
        Increment the storage of food by an unit. 
        """ 
        self.food_storage += 1  
