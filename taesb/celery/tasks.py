""" 
Implement the convenient tasks for Celery in the ants' empire. 
""" 
from .celery import app 

# Docs 
from typing import List, Dict 

@app.task 
def current_foods(global_map: Dict): 
    """
    Compute the quantity of foods in each anthill. 
    """ 
    # Identify the anthills 
    anthills = [anthill for anthill in global_map["anthills"]] 
    # and the foods 
    foods = [anthill["food_storage"] for anthill in anthills]
    return foods 
