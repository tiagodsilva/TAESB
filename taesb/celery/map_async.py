""" 
An async version of the map. 
""" 
# Import the map application 
from .celery import app 
from taesb.Map import Map 

def run_simulation(width: int, 
        height: int, 
        anthills: List[Tuple[str, int, int, int]], 
        foods: List[Tuple[int, int, int]], 
        food_update: int, 
        ants_fov: int, 
        pheromones_lifetime: int 
    ): 
    """ 
    Initialize a simulation of the ants' intergalatic empires. 
    """ 
    world = Map( 
            width=width, 
            height=heigth,
            anthills=anthills,
            foods=foods,
            food_update=food_update,
            ants_fov=ants_fov,
            pheromones_lifetime=pheromones_lifetime, 
            verbose=False
    ) 

    world.run() 
