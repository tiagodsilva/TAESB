""" 
Simulate a map with ants. 
""" 
import os 
import sys 
# Insert the src directory in the system path 
sys.path.append("src") 
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
    food_update = 3 
    ants_fov = 1 
    verbose = True 
    
    world = Map(width, height, anthills, foods, food_update, ants_fov, verbose) 
    
    world.run() 

if __name__ == "__main__": 
    main() 
