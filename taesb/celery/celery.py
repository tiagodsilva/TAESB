"""
Implement a Celery application for the extensions of the ants' empire. 
""" 
from celery import Celery 

app = Celery("taesb", 
        broker="amqp://", 
        include=["taesb.celery.map_async"])

if __name__ == "__main__": 
    app.start() 
