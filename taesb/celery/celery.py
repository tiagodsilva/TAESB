""" 
Implement the features of the Flask application. 
""" 
from celery import Celery 
import os 

# Docs 
from typing import Dict 

# Instantiate an application 
app = Celery( 
        main="taesb", 
        broker=os.environ["BROKER_URL"], 
        include=["taesb.celery.tasks"]
)
app.autodiscover_tasks() 

if __name__ == "__main__": 
    app.start()
