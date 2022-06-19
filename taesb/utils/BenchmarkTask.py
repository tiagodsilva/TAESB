""" 
Methods to benchmark Celery's workers. 
""" 
# Sys 
import os 
import sys 
import glob 

# Celery 
from celery import Task 

# PostgreSQL 
import psycopg2 

class BenchmarkTask(Task): 
    """ 
    A class to benchmark Celery's workers. 
    """ 
    
    _db_conn: psycopg2.extensions.connection = None 

    def init_db(self): 
        """ 
        Initialize the access to the data base. 
        """ 
        self._db_conn = psycopg2.connect( 
                host=os.environ["POSTGRESQL_HOST"], 
                user=os.environ["POSTGRESQL_USER"], 
                password=os.environ["POSTGRESQL_PASSWORD"], 
                database=os.environ["POSTGRESQL_DATABASE"] 
        ) 

    @property 
    def db_conn(self): 
        """ 
        The pointer to the database. 
        """ 
        if self._db_conn is None: 
            self.init_db() 
        return self._db_conn 
    
    def update_benchmarks(self, scenario_id: str, n_processes: int): 
        """ 
        Update the pipeline's benchmarking. 
        """ 
        # Generate the current query 
        query = """SET TIMEZONE='America/Los_angeles'; 
INSERT INTO benchmarks 
        (scenario_id, 
        computed_at, 
        n_processes) 
    VALUES 
        ('{scenario_id}', 
        now(), 
        {n_processes});""".format(scenario_id=scenario_id, 
                n_processes=n_processes) 
    
        # Instantiate a cursor 
        cursor = self.db_conn.cursor() 
        cursor.execute(query) 
        cursor.close() 
        
        # Commit the updates to the database 
        self.db_conn.commit() 
