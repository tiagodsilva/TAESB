""" 
Methods to benchmark Celery's workers. 
""" 
# Sys 
import os 
import sys 
import glob 

# Celery 
from celery import Task 

class BenchmarkTask(task): 
    """ 
    A class to benchmark Celery's workers. 
    """ 
    # The boundaries of the execution time 
    _start_times: Dict[str, int] = None 
    
    def init_db(self): 
        """ 
        Initialize the access to the data base. 
        """ 
        self.db_conn = psyocpg2.connect( 
                host=os.enviorn["POSTGRESQL_HOST"], 
                user=os.environ["POSTGRESQL_USER"], 
                password=os.environ["POSTGRESQL_PASSWORD"], 
                database=os.environ["POSTGRESQL_DATABASE"] 
        ) 

    def update_time(self, scenario_id: str): 
        """ 
        Update the pipeline's benchmarking. 
        """ 
        # Generate the current query 
        query = """INSERT INTO benchmarks 
        (scenario_id, 
        current_time) 
    VALUES 
        ({scenario_id}, 
        now());""".format(scenario_id=scenario_id) 
        
        # Instantiate a cursor 
        cursor = self.db_conn.cursor() 
        cursor.execute(query) 
        cursor.close() 
        
        # Commit the updates to the database 
        self.db_conn.commit() 
