# Start from the base 
FROM ubuntu:20.04

ARG CELERY_PACKAGE=5.2.0 
ARG PSYCOPG2_BINARY=2.9.3 

MAINTAINER TJG

# Install system deps 
RUN apt-get -yqq update 
RUN apt-get -yqq install python3-pip python3-dev 
RUN apt-get -yqq install wget unzip
RUN mkdir -p /TAESB
# Clone the repository 
ADD . /celery/

# Fetch apps 
RUN pip install celery==${CELERY_PACKAGE} \
	psycopg2-binary==${PSYCOPG2_BINARY} 

ENV POSTGRESQL_HOST=database-postgres-tjg.cvb1csfwepbn.us-east-1.rds.amazonaws.com
ENV POSTGRESQL_USER=postgres
ENV POSTGRESQL_PASSWORD=passwordpassword
ENV POSTGRESQL_DATABASE=operational_tjg
ENV BROKER_URL=amqps://username:passwordpassword@b-7182fca9-4c07-4bfa-be01-72310cb18d60.mq.us-east-1.amazonaws.com:5671

CMD ["celery", "-A", "celery", "worker", "-l", "INFO"]
