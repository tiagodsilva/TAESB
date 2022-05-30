""" 
Implement the features of the Flask application. 
""" 
from flask import Flask, jsonify 
from celery import Celery 

import os 

# Docs 
from typing import Dict 

app = Flask("taesb")
app.config.update(CELERY_CONFIG={
    "broker_url":"amqp://",
    "result_backend":"redis://",
})

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

# Instantiate a flask application 
celery = make_celery(app) 

@celery.task
def current_foods(global_map: Dict): 
    """
    Compute the quantity of foods in each anthill. 
    """ 
    # Identify the anthills 
    anthills = [anthill for anthill in global_map["anthills"]] 
    # and the foods 
    foods = [anthill["food_storage"] for anthill in anthills]
    return foods 

@app.route("/foods/<task_id>")
def flask_foods(task_id: int):
    """ 
    Capture the foods at each anthill in a Flask environment. 
    """
    task = current_foods.AsyncResult(task_id)
    
    print(task) 
    if task.state == "PENDING":
        response = {
                "state": task.state,
                "current": 0,
                "total": 1,
                "status": "Pending"
        }
    elif task.state != "FAILURE":
        response = {
                "state": task.state,
                "current": task.info 
        }
    else:
        response = {
                "state": task.state,
                "current": 1,
                "total": 1,
                "status": str(task.info) # The exception raised 
            }
    return jsonify(response)

@app.route("/", methods=["GET", "POST"])
def index(): 
    return "Hi!" 

if __name__ == "__main__": 
    app.run(debug=True, 
            port=int(os.getenv("PORT", 4444)))
