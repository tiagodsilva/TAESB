""" 
Implement the ant class.
""" 
from .Anthill import Anthill 
from .Tile import Tile 
from .Food import Food 

import numpy as np 
import uuid 

from typing import Tuple, Union, TypeVar 

Map = TypeVar("Map") 

class Ant(object): 
    """ 
    A class that mimics the behavior of an ant. 
    """ 

    x_pos: int # The horizontal location 
    y_pos: int # The vertical location 
    map: Map # The global map subjacent to the simulation 
    has_food: bool # Whether the ant has food 
    captured_food: int # The amount of food brough by this ant 
    colony: Anthill # The colony correspondent to this ant 
    identifier: str # Essentially unique identifier 

    def __init__(self, colony: Anthill, global_map: Map): 
        """ 
        Constructor method for an ant at an Anthill `Anthill`. 
        """ 
        self.x_pos = colony.x_pos 
        self.y_pos = colony.y_pos 
        self.map = global_map 
        self.colony = colony 

        self.has_food = False 
        
        # The amo:unt of food brought by this ant 
        self.captured_food = 0 

        self.identifier = uuid.uuid4() 

    def eat(self, food: Food): 
        """ 
        Eat a unit of the food `Food`. 
        """ 
        # Returns False if there is no food available 
        self.has_food = food.consume() 

    def _move_segment(self, dest: Union[Tile, Anthill]) -> Tuple[int, int]: 
        """ 
        Move across a segment that starts at (`self.x_pos`, `self.y_pos`) 
        and is bounded by `dest`. 
        """ 
        # Implement a simple heuristic to move: 
        # + if source and dest are in the same vertical axis, 
        #       move vertically; 
        # + otherwise, move horizontally until this is True. 
        
        if self.x_pos != dest.x_pos: 
            # If dest.x_pos > source.x_pos, we should move to the right; 
            # the movement vector equals `(1, 0)`; otherwise, to left, and `(-1, 0)` is 
            # the movememnt vector. 
            return (np.sign(dest.x_pos - self.x_pos), 0) 
        else: 
            # Correlatively to the scenario in which the vertical 
            # locations are distinct; however, this contemplates the 
            # vertical axis 
            return (0, np.sign(dest.y_pos - self.y_pos))  
    
    def move_colony(self): 
        """ 
        Execute a move to the colony. 
        """ 
        # Check if it is already in the colony 
        if (self.x_pos, self.y_pos) in self.map.anthills.keys(): 
            self.map.anthills[(self.x_pos, self.y_pos)].increment_food() 
            self.has_food = False 
            self.captured_food += 1 
        
        # Compute the direction of the movement 
        direction = self._move_segment(self.colony) 
        
        self.move(*direction) 
    
    def move_randomly(self): 
        """ 
        Move randomly in a direction. 
        """ 
        # Check whether the ant should move horizontally 
        x_direction, y_direction = self.map.random_tile(self.x_pos, self.y_pos) 
        self.move(x_direction, y_direction) 

    def move(self, x_direction: int, y_direction: int): 
        """ 
        Move across a direction (`x_direction`, `y_direction`), with the guarantees 
        that 

        + that the ant does not transcends the map's boundaries, 
        + that the ant does not move to a tile with food, 
        + and that the ant does not move to an enemy's anthill. 
        """ 
        # Compute the next location 
        next_x = self.x_pos + x_direction 
        next_y = self.y_pos + y_direction 

        if self.map.off_boundaries(next_x, next_y) or \
                self.map.is_food(next_x, next_y) or \
                self.map.is_enemy_anthill(next_x, next_y, self.colony.name): 
            return 
    
        # Remove the ant from the current tile 
        self.map.tiles[self.x_pos][self.y_pos].total_ants -= 1 
        self.x_pos = next_x 
        self.y_pos = next_y 
        # Insert the ant in the current tile 
        self.map.tiles[self.x_pos][self.y_pos].total_ants += 1 

    def release_pheromone(self): 
        """ 
        Release pheromone at the current location. 
        """ 
        self.map.release_pheromone(self.x_pos, self.y_pos) 

    def stage(self, iteration: int): 
        """ 
        Execute an action in the iteration `iteration`. 
        """ 
        # Check if the ant has food and should move to the colony 
        if self.has_food: 
            self.release_pheromone()  
            self.move_colony() 
            return 
         
        # Then, check if there is food near 
        food = self.map.nearest_food(self.x_pos, self.y_pos) 
        if food is not None and self.map.foods[(food.x_pos, food.y_pos)].volume >= 1: 
            self.move(*self._move_segment(food)) 
            # If the food is in a neighboring tile, eat it
            if np.abs(food.x_pos - self.x_pos) + np.abs(food.y_pos - self.y_pos) <= 1: 
                self.eat(self.map.foods[(food.x_pos, food.y_pos)]) 
            return 

        # Else, move randomly 
        self.move_randomly() 
