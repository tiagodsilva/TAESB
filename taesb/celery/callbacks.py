""" 
Implement the convenient tasks for Celery in the ants' empire. 
""" 
from .celery import app 

# Docs 
from typing import List 

@app.task 
def current_foods(foods: List[int]): 
    """
    Compute the quantity of foods in each anthill. 
    """ 
    return foods 
