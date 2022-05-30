"""
Implement a Celery application for the extensions of the ants' empire. 
""" 
from celery import Celery 

app = Celery("ants", 
        broker="amqp://", 
        include=["src.celery", 
            "src.Map"])

if __name__ == "__main__": 
    app.start() 
