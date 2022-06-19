""" 
Execute multiple instances of the simulation. 
""" 
# Sys 
import os 
import sys 
import glob 
import subprocess 

# Docs 
import typing 
import argparse 

# Default values 
FOODS = 99 
INSTANCES = 3 

def run(n_instances: int, max_foods: int=FOODS, 
        compute_benchmarks: bool=False): 
    """ 
    Execute `n_instances` instances, each with a criterion equal 
    to `max_foods` foods for winning the simulation. 
    """ 
    # Current instance 
    curr_instance = -1 
    while curr_instance < n_instances: 
        cmd = "python main.py --max_foods {max_foods}".format( 
                max_foods=max_foods) 

        # Check if we should compute benchmarks 
        if compute_benchmarks: 
            cmd += " --benchmarks" 

        subprocess.Popen(cmd, shell=True) 
        curr_instance += 1 

def parse_args(parser: argparse.ArgumentParser): 
    """ 
    Parse command line parameters. 
    """ 
    parser.add_argument("--instances", 
        help="Quantity of instances to be executed", 
        type=int, default=INSTANCES) 
    parser.add_argument("--foods", 
            help="Criterion for winning the simulation", 
            type=int, default=FOODS) 
    parser.add_argument("--benchmarks", 
            help="Whether to compute benchmarks for the ETL pipeline.", 
            action="store_true") 
    # Parse the parameters 
    args = parser.parse_args() 

    # Return the parameters 
    return args 

def main(msg: str=None): 
    """ 
    Execute multiple scenarios from the simulation. 
    """ 
    parser = argparse.ArgumentParser(
            description="Execute multiple simulations."
    ) 
    args = parse_args(parser) 
    # Apply the simulations 
    run(args.instances, args.foods, args.benchmarks)  

    if msg: 
        print(msg) 

if __name__ == "__main__": 
    main("The processes are in execution") 


