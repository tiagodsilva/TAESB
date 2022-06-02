# The ants are striking again! 

Execute 

``` 
python main.py 
``` 

to contemplate their machinations. 

# References 

+ This [book](https://www.distributed-systems.net/index.php/books/ds3/), by Maarten van Steen, provides an amenable introduction to the field of distributed systems, covering, in Chapter 4, particularly, transient (MPI, for instance) and persistent (as the message-queuing systems) communication middlewares. 
+ The Chapter 3 of this [book](https://www.cs.usfca.edu/~peter/ipp2/index.html), on the other hand, by Peter Pacheco, introduces sympatically the idyiossincrasies of parallel programming in distributed systems; emphatically, it explicitly distinguishes the inconveniences tied to the distribution of processes across multiple machines. 

# Install 

In this application, we use NumPy, Celery, Flask, and Redis; execute 

``` 
pip install numpy celery flask redis 
``` 

to install them. Then, apply 

``` 
chmod +x redis.sh 
./redis.sh 
``` 

to initialize Redis. In this sense, use the command 

``` 
celery -A taesb worker --loglevel=INFO 
``` 

at the root directory of this repository to start the program. Execute, in the next step, 

``` 
python main.py 
``` 

to start a producer. 

You should also install PostgreSQL; for this, execute 

``` 
sudo apt-get install wget ca-certificates
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib
``` 

and then 

``` 
sudo su - postgres
``` 

to enjoy root access to the data base. In this scenario, execute 

``` 
psql
CREATE USER {username} WITH PASSWORD '{password}'; 
``` 

to instantiate an user. In Python, install the `psycopg2` package with 

``` 
pip install psycopg2-binary
``` 

to access the database; the commands 

```py 
import psycopg2 

conn = psycopg2.connect(database="postgres", 
	user="{username}",
	password="{password}" 
) 
``` 
	
will provide you this access. 

You should, moreover, use a JDBC Driver to execute PostgreSQL query in Spark; for this, execute 

``` 
wget https://jdbc.postgresql.org/download/postgresql-42.3.6.jar 
``` 

and update the `app.py` lines.  
