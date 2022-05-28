""" 
Implement the Tile class. 
""" 
from Pheromone import Pheromone 

from typing import List, TypeVar

Map = TypeVar("Map") 

class Tile(object): 
    """ 
    A class that emulates a tile, in which the ants walk. 
    """ 
    
    x_pos: int 
    y_pos: int 
    pheromones: List[Pheromone] 
    anthill_name: str 
    is_food: bool 
    map: Map 
    total_ants: int 
    
    def __init__(self, x_pos: int, y_pos: int, global_map: Map, total_ants: int = 0): 
        """ 
        Constructor method for a Tile at location (`x_pos`, `y_pos`). 
        """ 
        self.x_pos = x_pos 
        self.y_pos = y_pos 

        # The quantity of pheromeones in this tile 
        self.pheromones = list() 

        # The name of the anthill, if it contains it 
        self.anthill_name = None 
        # Whether it contains food 
        self.is_food = False 
        
        # The total quantity of ants in this tile 
        self.total_ants = total_ants 
        
        # The map in which this tile is inserted in the game's idyiossincrasies 
        self.map = global_map 

    def increment_pheromones(self, iteration: int, lifetime: int): 
        """ 
        Increment the pheromeone's volume at the current tile, inserting 
        a pheromone at iteration `iteration` with lifetime `lifetime`. 
        """ 
        self.pheromones.append(Pheromone(lifetime, iteration)) 
    
    def remove_pheromones(self, iteration: int): 
        """ 
        Check what pheromones should be removed at iteration `iteration`.
        """ 
        # Iterate across the pheromones in the curren tile 
        for i, pher in enumerate(self.pheromones): 
            kill = pher.kill(iteration) 
            
            # If the pheromone should be extracted, extract it 
            if kill: 
                self.pheromones.pop(i) 

    def print(self) -> str: 
        """ 
        Print the tile's attributes and returns a string. 
        """ 
        ants = self.total_ants 
        if self.anthill_name is not None: 
            # Compute the amount of storage and the quantity of ants 
            storage = self.map.anthills[(self.x_pos, self.y_pos)].food_storage 
            return "|A,{storage},{ants}|".format(storage=storage,ants=ants)
        elif self.is_food: 
            # Compute the volume of the food 
            volume = self.map.foods[(self.x_pos, self.y_pos)].volume 
            return "|F,{volume},{ants}|".format(volume=volume, ants=ants) 
        else: 
            return "|{ants}|".format(ants=ants) 
