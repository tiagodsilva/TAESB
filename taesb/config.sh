# /usr/bin/bash  
# Consolidate Spark configurations. 
export SPARK_JARS="postgresql-42.3.6.jar" 
export SPARK_MASTER="local[8]" 
export SPARK_UI_ENABLED="false" 
export SPARK_DRIVER_HOST="localhost"
export POSTGRESQL_HOST="database-postgres-tjg.cvb1csfwepbn.us-east-1.rds.amazonaws.com" 
export POSTGRESQL_USER="postgres" 
export POSTGRESQL_PASSWORD="passwordpassword"
export POSTGRESQL_DATABASE="operational_tjg" 
# "localhost" 
export BROKER_URL="amqps://username:passwordpassword@b-7182fca9-4c07-4bfa-be01-72310cb18d60.mq.us-east-1.amazonaws.com:5671" 
