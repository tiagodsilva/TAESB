""" 
Simulate a map with ants. 
""" 
import os 
from taesb.Map import Map 

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
    pheromones_lifetime = 9
    verbose = True 
    
    print("There are a triplet of distinct tiles:") 
    print("+ the usual tiles, with values (x, y), in which", 
            "x equals the pheromones' intensity and y, the quantity of ants;") 
    print("+ the anthills, with the format (A, x, y), with x equal to the", 
            "volume of the food storage and y to the quantity of ants;") 
    print("+ and (F, x), the foods tiles, characterized by the food volume, x.")
    world = Map(width, height, anthills, foods, food_update, ants_fov, 
            pheromones_lifetime, verbose) 
    
    world.run(max_foods=19) 

if __name__ == "__main__": 
    main() 
