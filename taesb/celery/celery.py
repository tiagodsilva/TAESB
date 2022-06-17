""" 
Implement the features of the Flask application. 
""" 
from celery import Celery 
from ..SparkConf import BROKER_URL  
import os 

# Docs 
from typing import Dict 

# Instantiate an application 
app = Celery( 
        main="taesb", 
        broker=BROKER_URL, 
        include=["taesb.celery.tasks"]
)
app.autodiscover_tasks() 

if __name__ == "__main__": 
    app.start()
