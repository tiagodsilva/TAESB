""" 
Simulate a map with ants. 
""" 
import os 
import taesb 
import taesb.celery.tasks as tasks 
import taesb.config as config 

import argparse 

def anthills(s: str): 
    """ 
    Consolidate the data in anthills, which has the format 

    name,x,y,ants;name,x,y,ants;...;name,x,y,ants 

    It computes, then, 

    [(name,x,y,ants),(name,x,y,ants),...,(name,x,y,ants)] 
    """ 
    try: 
        # Split the data with the delimiter ";" 
        csvs = s.split(";") # List of csvs 
        
        # Convert csv to a list of tuples 
        s = [tuple(csv.split(",")) for csv in csvs] 
        # and use an appropriate type 
        s = [(str(name), int(x), int(y), int(ants)) for name, x, y, ants in \
                s] 

        return s 
    except Exception as e: 
        raise argparse.ArgumentTypeError("The parameter `anthills` should have specifically \
the format `name[str],x[int],y[int],ants[int];...;name[str],x[int],y[int],ants[int]`") 
 
def foods(s: str): 
    """ 
    Convert a list of CSVs with the format 

    x,y,volume;...;x,y,volume 

    to a list of tuples with the format 

    [(x,y,volume),...(x,y,volume)] 
    """ 
    try: 
        # Split the data with ";" 
        csvs = s.split(";") 
        # and further split the csvs 
        s = [tuple(csv.split(",")) for csv in csvs] 

        # Cast the parameters 
        s = [(int(x), int(y), int(volume)) for x, y, volume in s] 

        return s 
    except: 
        raise argparse.ArgumentTypeError("The `foods` parameter should have the format \
equivalent to x[int],y[int],volume[int];...;x[int],y[int],volume[int]") 

def parse_args(parser: argparse.ArgumentParser): 
    """ 
    Parse the command line parameters for the parser `parser`. 
    """ 
    parser.add_argument("--width", help="The width of the map.", 
            type=int, default=config.WIDTH) 
    parser.add_argument("--height", help="The height of the map.", 
            type=int, default=config.HEIGHT) 
    parser.add_argument("--anthills", help="The distributions of the anthills across the map, \
with the format name[str],x[int],y[int],ants[int];...;name[str],x[int],y[int],ants[int].", 
            type=anthills, default=config.ANTHILLS) 
    parser.add_argument("--foods", help="The distributions of the foods across the map, \
with the format x[int],y[int],volume[int];...;x[int],y[int],volume[int].", 
            type=foods, default=config.FOODS) 
    parser.add_argument("--food_update", help="The rate with which we update the foods in the map.", 
            type=int, default=config.FOOD_UPDATE) 
    parser.add_argument("--ants_fov", help="The ants' field of view.", 
            type=int, default=config.ANTS_FOV) 
    parser.add_argument("--pherlt", help="The pheromones' lifetime.", 
            type=int, default=config.PHEROMONES_LIFETIME) 
    parser.add_argument("--verbose", help="Whether to display the map's characteristics.", 
            type=bool, default=False) 
    parser.add_argument("--max_foods", help="The quantity of foods for an anthill be characterized as the winner", 
            type=int, default=config.MAX_FOODS) 
    return parser.parse_args() 

def main(args): 
    """ 
    Initialize the simulation. 
    """ 
    callbacks = taesb.CallbacksList( 
          initialize = tasks.update_scenarios,      
          callbacks = [tasks.current_foods, 
            tasks.update_scenarios, 
            tasks.update_ants, 
            tasks.update_foods, 
            tasks.update_anthills 
        ] 
    ) 

    # Instantiate a map 
    world = taesb.Map( 
            width=args.width, 
            height=args.height,
            anthills=args.anthills,
            foods=args.foods,
            food_update=args.food_update,
            ants_fov=args.ants_fov,
            pheromones_lifetime=args.pherlt, 
            verbose=args.verbose
        ) 

    world.run(max_foods=args.max_foods, 
            callbacks=callbacks) 

if __name__ == "__main__": 
    parser = argparse.ArgumentParser(description="Parse the parameters to start the invasion of the ants.")
    args = parse_args(parser) 
    main(args) 
