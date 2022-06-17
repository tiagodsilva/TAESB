# Start from the base 
FROM ubuntu:20.04

ARG CELERY_PACKAGE=5.2.0 
ARG PSYCOPG2_BINARY=2.9.3 
ARG PANDAS_PACKAGE=1.4.2 
ARG PYSPARK_VERSION=3.2.1

MAINTAINER My, Myself and I 

# Install system deps 
RUN apt-get -yqq update 
RUN apt-get -yqq install python3-pip python3-dev 

# Clone the repository 
ADD . /opt/taesb
WORKDIR /opt/taesb 

# Fetch apps 
RUN pip install celery==${CELERY_PACKAGE} \
	psycopg2-binary==${PSYCOPG2_BINARY} \
	pandas==${PANDAS_PACKAGE} \
	pyspark==${PYSPARK_VERSION} 

