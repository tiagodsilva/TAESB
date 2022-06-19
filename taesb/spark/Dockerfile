FROM ubuntu:20.04

ARG OPEN_JDK_VERSION=11
ARG PYTHON=python3
ARG POSTGRESQL=postgresql-42.3.6
ARG SPARK_PACKAGE=spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
ARG PYSPARK_PACKAGE=pyspark==3.2.1
ARG PSYCOPG2_PACKAGE=psycopg2-binary
ARG PANDAS_PACKAGE=pandas==1.4.2

# Install OpenJDK 
RUN \
	apt-get update && \
	apt-get install -yqq openjdk-${OPEN_JDK_VERSION}-jdk && \
	rm -rf /var/lib/apt/lists/* 

# Install wget 
RUN \
	apt-get update && \
	apt-get install -yqq wget 

# Install Python 
RUN \
	apt-get update && \
	apt-get install -yqq ${PYTHON} ${PYTHON}-dev \
	${PYTHON}-pip ${PYTHON}-virtualenv && \
	rm -rf /var/lib/apt/lists/* 

# Install PySpark
RUN \
	wget "https://www.apache.org/dyn/closer.lua/spark/${SPARK_PACKAGE}?action=download" -O - | tar -xzC /tmp; \
	archive=$(basename "${SPARK_PACKAGE}") bash -c "mv -v /tmp/\${archive/%.tgz/} /spark"; 

RUN \
	pip install --no-cache-dir ${PYSPARK_PACKAGE};

# Install PostgreSQL Python's API 
RUN \
	${PYTHON} -m pip install --upgrade pip && \
	${PYTHON} -m pip install ${PSYCOPG2_PACKAGE} ${PANDAS_PACKAGE}  

# Capture JDBC driver 
RUN wget https://jdbc.postgresql.org/download/${POSTGRESQL}.jar

# Add the current file to the Docker's virtualization
RUN wget https://osf.io/68b97/download -O SparkSubmit.py 

ENV POSTGRESQL_HOST=database-postgres-tjg.cvb1csfwepbn.us-east-1.rds.amazonaws.com
ENV POSTGRESQL_USER=postgres
ENV POSTGRESQL_PASSWORD=passwordpassword
ENV POSTGRESQL_DATABASE=operational_tjg
ENV BROKER_URL=amqps://username:passwordpassword@b-7182fca9-4c07-4bfa-be01-72310cb18d60.mq.us-east-1.amazonaws.com:5671
ENV SPARK_JARS=${POSTGRESQL}.jar
ENV SPARK_UI_ENABLED=false 
# Execute the processing 

CMD ["spark-submit", \
	"--packages", \	
	"org.postgresql:postgresql:42.3.6", \
	"SparkSubmit.py"] 
