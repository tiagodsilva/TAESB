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

at the root directory of this repository to start the program. Circumstantially, execute 

``` 
python taesb/celery/app.py
``` 

to start the Flask application, and use your browser to display the site `http://127.0.0.1:4444/`. Execute, in the next step, 

``` 
python main.py 
``` 

to start a producer. 

