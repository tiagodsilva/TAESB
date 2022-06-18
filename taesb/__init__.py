from .agents.Map import Map
from .utils.CallbacksList import CallbacksList 

# Environment variables 
import os 
os.environ["SPARK_JARS"] = "postgresql-42.3.6.jar" 
os.environ["SPARK_MASTER"] = "local[8]" 
os.environ["SPARK_UI_ENABLED"] = "false" 
os.environ["SPARK_DRIVER_HOST"] = "localhost"
os.environ["POSTGRESQL_HOST"] = "database-postgres-tjg.cvb1csfwepbn.us-east-1.rds.amazonaws.com" 
os.environ["POSTGRESQL_USER"] = "postgres" 
os.environ["POSTGRESQL_PASSWORD"] = "passwordpassword"
os.environ["POSTGRESQL_DATABASE"] = "operational_tjg" 
# "localhost" 
os.environ["BROKER_URL"] = "amqps://username:passwordpassword@b-7182fca9-4c07-4bfa-be01-72310cb18d60.mq.us-east-1.amazonaws.com:5671" 

