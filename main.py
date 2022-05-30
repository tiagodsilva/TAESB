""" 
Simulate a map with ants. 
""" 
import os 
from taesb.Map import Map 

import argparse 
def main(args): 
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
    pheromones_lifetime = 9
    max_foods = 19 
    verbose = True 
    
    world = Map( 
            width=args.width, 
            height=args.height,
            anthills=args.anthills,
            foods=args.foods,
            food_update=args.food_update,
            ants_fov=args.ants_fov,
            pheromones_lifetime=args.pherlt, 
            verbose=args.verbose
        ) 

    world.run(max_foods=args.max_foods) 

if __name__ == "__main__": 
    main() 
