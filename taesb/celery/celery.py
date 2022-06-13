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
        broker=\
                "amqps://TJG:passwordpassword@b-30957be3-40c4-4c26-9193-8bcdead4625c.mq.us-east-1.amazonaws.com:5671",
        include=["taesb.celery.tasks"]
)
app.autodiscover_tasks() 

if __name__ == "__main__": 
    app.start()
