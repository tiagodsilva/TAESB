""" 
Implement the Map class. 
""" 
from typing import List, Tuple, Dict, Any, Union 
from .Anthill import Anthill
from .Ant import Ant 
from .Tile import Tile 
from .Food import Food 

from ..utils.CallbacksList import CallbacksList 

import time 
import numpy as np 
import uuid 

class Map(object): 
    """ 
    A class that simulates a map in which the ants play a game. 
    """ 

    width: int 
    height: int 
    anthills: Dict[Tuple[int, int], Anthill] 
    foods: Dict[Tuple[int, int], Food] 
    ants_fov: int 
    ants: List[Ant] 
    iteration: int 
    verbose: bool 
    food_update: int 
    pheromones_lifetime: int 
    json: Dict[str, Any] 
    identifier: str 
    execution_time: Union[int, str]  

    def __init__(self, 
            width: int, 
            height: int, 
            anthills: List[Tuple[str, int, int, int]], # name, x, y, ants 
            foods: List[Tuple[int, int, int]], # x, y, initial_volume  
            food_update: int, # x, y 
            ants_fov: int, 
            pheromones_lifetime: int, 
            verbose: bool = False): 
        """ 
        Constructor method for a map with width `width` and height `height`. 
        """ 
        self.width = width 
        self.height = height 
        
        # Initialize the objects in the map 
        self.tiles = list()  
        self.anthills = {(spec[1], spec[2]):Anthill(*spec) for spec in anthills}
        self.foods = {(spec[0], spec[1]):Food(*spec) for spec in foods} 
        self.food_update = food_update 

        self.initialize_tiles(anthills, foods) 
    
        # Initialize the ants in the game 
        self.ants_fov = ants_fov 
        self.ants = list() 
        self.initialize_ants(anthills) 
        
        # Pheromones (which die periodically) life time 
        self.pheromones_lifetime = pheromones_lifetime 

        # A flag for the current iteration 
        self.iteration = 0 
        
        # An object should be JSON serializable to be compatible with Celery's communication 
        self.json = dict() 

        # Whether we should print the map 
        self.verbose = verbose 
        
        self.identifier = uuid.uuid4() 
            
        # The execution time of the scenario, measure with respect to the 
        # quantity of iterations 
        self.execution_time = "NULL"

    def initialize_tiles(self, 
            anthills: List[Tuple[str, int, int, int]], 
            foods: List[Tuple[int, int]]): 
        """ 
        Initialize the tiles in the simulation, 
        """ 
        for x in range(self.width): 
            self.tiles.append(list()) 
            for y in range(self.height): 
                # Instantiate a tile in the game 
                tile = Tile(x, y, self) 

                # Check if it should be an anthill 
                tile.anthill_name = self.anthills[(x, y)].name \
                        if (x, y) in self.anthills.keys() else None 

                # Check if the tile should be a food 
                if tile.anthill_name is None: 
                    tile.is_food = any([(x, y) == (food[0], food[1]) for food in foods]) 
                else: 
                    tile.total_ants = self.anthills[(x, y)].initial_ants 
        
                self.tiles[x].append(tile) 
    
    def initialize_ants(self, anthills: List[Anthill]): 
        """ 
        Initialize the ants in the game at each anthill in `anthills`. 
        """ 
        for anthill in self.anthills.values(): 
            # Compute the initial quanity of ants 
            n_ants = anthill.initial_ants 
            # The field of view (self.ants_fov) is not an attribute of the ants; 
            # it is shared across all species and, then, it is globally 
            # available at this class 
            self.ants += [Ant(anthill, self) for ant in range(n_ants)] 
    
    def nearest_food(self, x_pos: int, y_pos: int) -> Tile: 
        """
        Compute the nearest tile with food at the taxicab metric ball with 
        radius `self.ants_fov`.  
        """ 
        # Iterate across the neighboring tiles 
        for x in range(x_pos - self.ants_fov, x_pos + self.ants_fov + 1): 
            for y in range(y_pos - self.ants_fov, y_pos + self.ants_fov + 1): 
                # Check if the current coordinates are within boundaries 
                if self.off_boundaries(x, y) or (x == x_pos and y == y_pos): 
                    continue 

                if self.tiles[x][y].is_food: 
                    # Return a tile with food 
                    return self.tiles[x][y] 
        
        return None 
    
    def is_food(self, x_pos: int, y_pos: int) -> bool: 
        """ 
        Assert whether the tile at (`x`, `y`) contains food. 
        """ 
        if self.tiles[x_pos][y_pos].is_food: 
            return True 
        else: 
            return False 

    def is_enemy_anthill(self, x_pos: int, y_pos: int, anthill_name: str) -> bool: 
        """ 
        Check whether the tile at (`x_pos`, `y_pos`) is an enemy anthill. 
        """ 
        if self.tiles[x_pos][y_pos].anthill_name is not None: 
            # The anthill is an enemy if its name is different from the 
            # ant's anthill 
            return self.anthills[(x_pos, y_pos)].name != anthill_name 
        else: 
            return False 
    
    def off_boundaries(self, x_pos: int, y_pos: int): 
        """ 
        Check whether the coordinates `x_pos` and `y_pos` transcends 
        the map's boundaries. 
        """ 
        if x_pos >= self.width or x_pos < 0 or y_pos >= self.height or y_pos < 0: 
            return True 
        else: 
            return False 

    def run(self, 
            n_iterations: int = None, 
            max_foods: int = None, 
            callbacks: CallbacksList = None): 
        """
        Simulate the game for `n_iterations` iterations if it is not None 
        and forever otherwise. If an anthill gather more than `max_foods` 
        units of foods, it wins the simulation. 
        """ 
        # Execute the game 
        if callbacks: 
            callbacks.start(self) 

        while n_iterations is None or self.iteration < n_iterations: 
            # Each ant executes its movement 
            for ant in self.ants: 
                ant.stage(self.iteration) 
            
            # Iterate across the tiles and update their states 
            self.remove_pheromones() 
            
            # Restore foods 
            self.restore_foods() 

            # Check if an anthill won the simulation 
            winners = self.check_winners(max_foods) 
            
            if winners is not None: 
                if self.verbose: 
                    print("The winner(s) is(are) {winners}!".format( 
                        winners=winners)
                    ) 
                break 
            self.iteration += 1 
            
            if callbacks: 
                callbacks.stage_update(self) 

            if self.verbose: 
                self.print() 
                time.sleep(1)  


        # Compute the execution time 
        self.execution_time = self.iteration 

        # Update the database with the current data 
        if callbacks: 
            callbacks.stage_update(self)  

    def check_winners(self, max_foods: int): 
        """ 
        Check if an anthill won the simulation. 
        """ 
        if max_foods is None: 
            # If max foods is None, return None 
            return max_foods 

        # Otherwise, check if an anthill (or multiple anthills) 
        # won the simulation 

        winners = [anthill.name for anthill in self.anthills.values() if \
                anthill.food_storage > max_foods] 
        
        if len(winners) < 1: 
            # Returns None if currently there are no winners 
            return None 

        # Return the winners 
        return winners 

    def restore_foods(self): 
        """ 
        Restore the foods, if the iteration counter is a multiple of the 
        update rate. 
        """ 
        if self.iteration % self.food_update != 0: 
            return 

        # Restore the volume for each food in the map 
        for x, y in self.foods.keys(): 
            self.foods[(x, y)].restore() 

    def remove_pheromones(self): 
        """ 
        Remove the pheromones from each tile. 
        """ 
        for x in range(self.width): 
            for y in range(self.height): 
                # Remove the pheromones from the current tile 
                self.tiles[x][y].remove_pheromones(self.iteration) 

    def print(self): 
        """ 
        Print the current state of the map. 
        """ 
        # Capture each tile state in a string, and then print it 
        tiles_states = str() 

        for y in range(self.height): # For each row 
            for x in range(self.width): # For each column 
                tiles_states += self.tiles[x][y].print() 

            # Insert another line 
            tiles_states += "\n" 

        print(tiles_states) 
     
    def random_tile(self, x_pos: int, y_pos: int): 
        """ 
        Generate a random tile, with probability proportional to the 
        pheromones' intensity. 
        """ 
        neighbors = list() 
        pheromones = list() 
        total_neighbors = 0 

        # Capture the neighboring pheromones 
        for x in range(x_pos - 1, x_pos + 2): 
            for y in range(y_pos - 1, y_pos + 2): 
                # Check the boundary conditions and the positivity o the norm of the 
                # movement vector 
                if self.off_boundaries(x, y) or (x == x_pos and y == y_pos): 
                    continue 

                # Compute the quantity of pheromones in the current tile 
                pheromones.append(len(self.tiles[x][y].pheromones) + 1) 
                neighbors.append((x, y)) 
                total_neighbors += 1 

        # Normalize the pheromones 
        total_pheromones = sum(pheromones) 
        pheromones = [pher/total_pheromones for pher in pheromones]

        # Compute the movement index 
        direction_index = np.random.choice(list(range(total_neighbors)), p=pheromones) 
        
        x_direction, y_direction = neighbors[direction_index] 
        x_direction, y_direction = x_direction - x_pos, y_direction - y_pos 
        return x_direction, y_direction 

    def release_pheromone(self, x_pos: int, y_pos: int): 
        """ 
        Release a pheromone at the tile in (`x_pos`, `y_pos`). 
        """ 
        self.tiles[x_pos][y_pos].increment_pheromones( 
                self.iteration, self.pheromones_lifetime 
        ) 

    def to_json(self): 
        """ 
        Serialize the map to JSON. 
        """ 
        # Serialize the foods 
        self.json["scenario_id"] = self.identifier 
        self.json["execution_time"] = self.iteration 
        self.json["active"] = int(self.execution_time == "NULL") 
    
        self.json["foods"] = [{ 
            "loc": (x, y), 
            "current_volume": self.foods[(x, y)].volume, 
            "identifier": self.foods[(x, y)].identifier, 
            "initial_volume": self.foods[(x, y)].initial_volume 
        } for (x, y) in self.foods]

        # Serialize the anthills 
        self.json["anthills"] = [{   
            "loc": (x, y), 
            "food_storage": self.anthills[(x, y)].food_storage, 
            "name": self.anthills[(x, y)].name, 
            "ants": self.anthills[(x, y)].initial_ants, 
            "identifier": self.anthills[(x, y)].identifier 
        } for (x, y) in self.anthills] 

        # Serialize the ants 
        self.json["ants"] = [{ 
            "loc": (int(ant.x_pos), int(ant.y_pos)), 
            "has_food": ant.has_food, 
            "captured_food": ant.captured_food, 
            "colony_name": ant.colony.name, 
            "identifier": ant.identifier, 
            "anthill_identifier": ant.colony.identifier  
        } for ant in self.ants] 
        
        return self.json 

            
