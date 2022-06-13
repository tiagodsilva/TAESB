""" 
Implement the features of the Flask application. 
""" 
from celery import Celery 
from ..utils.CelerySpark import CeleryPostgres

import os 

# Docs 
from typing import Dict 

# Instantiate a flask application 
broker_url = "amqps://username:passwordpassword@b-7182fca9-4c07-4bfa-be01-72310cb18d60.mq.us-east-1.amazonaws.com:5671"
app = CeleryPostgres( 
        main="taesb", 
        broker=broker_url, 
        include=["taesb.celery.tasks"]
)
app.autodiscover_tasks() 

if __name__ == "__main__": 
    app.start()
