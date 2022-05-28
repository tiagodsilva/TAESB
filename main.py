""" 
Simulate a map with ants. 
""" 
import os 
from Map import Map 

def main(): 
    """ 
    Initialize the simulation. 
    """ 
    # Instantiate a map 
    width = 9
    height = 9 
    anthills = [("Spartans", 1, 3, 15), ("Jedi", 5, 5, 99)] 
    foods = [(1, 5, 9)] 
    food_update = 15 
    ants_fov = 1 
    verbose = True 
    
    world = Map(width, height, anthills, foods, food_update, ants_fov, verbose) 
    
    world.run() 

if __name__ == "__main__": 
    main() 
