# Start from the base 
FROM ubuntu:20.04

ARG CELERY_PACKAGE=celery==5.2.0 
ARG PSYCOPG2_BINARY=psycopg2-binary==2.9.3 

MAINTAINER TJG

# Install system deps 
RUN apt-get -yqq update 
RUN apt-get -yqq install python3-pip python3-dev 
RUN apt-get -yqq install wget unzip
RUN mkdir -p /TAESB/taesb /TAESB/taesb/celery /TAESB/taesb/utils

# Clone the repository 
ADD ./taesb/celery /TAESB/taesb/celery
ADD ./taesb/utils /TAESB/taesb/utils

WORKDIR /TAESB/

# Fetch apps 
RUN pip install ${CELERY_PACKAGE} ${PSYCOPG2_BINARY} 

ENV POSTGRESQL_HOST=database-postgres-tjg.cvb1csfwepbn.us-east-1.rds.amazonaws.com
ENV POSTGRESQL_USER=postgres
ENV POSTGRESQL_PASSWORD=passwordpassword
ENV POSTGRESQL_DATABASE=operational_tjg
ENV BROKER_URL=amqps://username:passwordpassword@b-7182fca9-4c07-4bfa-be01-72310cb18d60.mq.us-east-1.amazonaws.com:5671

# In reality, you possibly should mitigate the loggings 
CMD ["celery", "-A", "taesb", "worker", "-l", "INFO"]
