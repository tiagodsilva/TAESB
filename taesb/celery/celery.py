"""
Implement a Celery application for the extensions of the ants' empire. 
""" 
from celery import Celery 
from flask import Flask 

def make_celery(app: Flask): 
    """ 
    Make a Celery application within Flask.  
    """ 
    celery = Celery(app.import_name) 
    celery.conf.update(app.config["CELERY_CONFIG"]) 

    class ContextTask(celery.Task): 
        def __call__(self, *args, **kwargs): 
            with app.app_context(): 
                return self.run(*args, **kwargs) 

    celery.Task = ContextTask 
    return celery 

flask_app = Flask("taesb") 
flask_app.config.update(CELERY_CONFIG={ 
    "broker":"amqp://", 
    "include":["taesb.celery.callbacks"] 
}) 

app = make_celery(flask_app) 

if __name__ == "__main__": 
    app.start() 
