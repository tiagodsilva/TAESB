""" 
Implement the features of the Flask application. 
""" 
from celery import Celery 

import os 

# Docs 
from typing import Dict 

# Instantiate a flask application 
app = Celery( 
        "taesb", 
        broker="amqp://", 
        backend="rpc://", 
        include=["taesb.celery.tasks"]
) 

app.autodiscover_tasks() 

if __name__ == "__main__": 
    app.start()
