""" 
Implement the Food class.
""" 
import uuid 

class Food(object): 
    """ 
    A food object, with which the ants interact. 
    """ 

    x_pos: int # The horizontal location 
    y_pos: int # The vertical location 
    initial_volume: int # The initial volume 
    volume: int # The current volume at each iteration 
    identifier: str # Essentially unique identifier 

    def __init__(self, x_pos: int, 
            y_pos: int, 
            initial_volume: int): 
        """ 
        Constructor method for a food at location (`x_pos`, `y_pos`) with 
        initial volume `initial_volume`. 
        """ 
        self.x_pos = x_pos 
        self.y_pos = y_pos 
        self.initial_volume = initial_volume 
        
        # The current volume, at each iteration of the simulation 
        self.volume = initial_volume 
        
        self.identifier = uuid.uuid4() 

    def consume(self) -> bool: 
        """ 
        Consume an unit of food. Returns True if the food was consumed, and False 
        if its volume is null.
        """ 
        if self.volume >= 1: 
            self.volume -= 1 
            return True 
        
        # Return False if the food's volume is null 
        return False 
    
    def restore(self): 
        """
        Restore the food's volume to the initial volume. 
        """ 
        self.volume = self.initial_volume 
