from celery import Celery 

app = Celery("proj", 
        broker="amqp://", 
        backend="rpc://", 
        include=["tasks.tasks"] 
    ) 

if __name__ == "__main__": 
    app.start() 
