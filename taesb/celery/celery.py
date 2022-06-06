""" 
Implement the features of the Flask application. 
""" 
from celery import Celery 
from ..utils.CelerySpark import CelerySpark 

import os 

# Docs 
from typing import Dict 

# Instantiate a flask application 
app = CelerySpark( 
        main="taesb", 
        broker="amqp://", 
        include=["taesb.celery.tasks"]
) 
app.autodiscover_tasks() 

if __name__ == "__main__": 
    app.start()
