""" 
Consolidate the DDL queries for the operational database.  
""" 
import psycopg2 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 
import sys 

# Drop tables; appropriate for debugging 
DROP_TABLES = """DROP TABLE IF EXISTS scenarios CASCADE; 
DROP TABLE IF EXISTS anthills CASCADE; 
DROP TABLE IF EXISTS ants CASCADE; 
DROP TABLE IF EXISTS foods CASCADE; 
DROP TABLE IF EXISTS stats_global CASCADE; 
DROP TABLE IF EXISTS stats_local CASCADE; 
DROP TABLE IF EXISTS stats_atomic CASCADE; 
DROP TABLE IF EXISTS benchmarks CASCADE;""" 

# Create the database; workaround to check table's existence 
DATABASE = "operational_tjg" 
DB_CREATE = "CREATE DATABASE {db_name};" \
        .format(db_name=DATABASE) 

# Create table for the scenarios 
DB_CREATE_SCENARIOS = """CREATE TABLE IF NOT EXISTS scenarios ( 
    scenario_id VARCHAR(256) PRIMARY KEY, 
    execution_time INT DEFAULT NULL, 
    active INT DEFAULT 1 
);""" 

# Create table for the anthills  
DB_CREATE_ANTHILLS = """CREATE TABLE IF NOT EXISTS anthills ( 
    anthill_id VARCHAR(256) PRIMARY KEY,
    name VARCHAR (99) NOT NULL, 
    food_storage INT, 
    total_ants INT, 
    scenario_id VARCHAR(256), 
    CONSTRAINT fk_scenario 
        FOREIGN KEY(scenario_id) 
            REFERENCES scenarios(scenario_id) 
);""" 

# Create table for the ants 
DB_CREATE_ANTS = """CREATE TABLE IF NOT EXISTS ants ( 
    ant_id VARCHAR(256) PRIMARY KEY, 
    captured_food INT, 
    searching_food INT, 
    anthill_id VARCHAR(256), 
    CONSTRAINT fk_anthill 
        FOREIGN KEY(anthill_id) 
            REFERENCES anthills(anthill_id) 
);""" 

# Create table for the foods 
DB_CREATE_FOODS = """CREATE TABLE IF NOT EXISTS foods ( 
    food_id VARCHAR(256) PRIMARY KEY, 
    initial_volume INT, 
    current_volume INT, 
    scenario_id VARCHAR(256), 
    CONSTRAINT fk_scenario 
        FOREIGN KEY(scenario_id)  
            REFERENCES scenarios(scenario_id) 
);""" 

# Create table for the statistics
DB_CREATE_GLOBAL = """CREATE TABLE IF NOT EXISTS stats_global (
    stat_id INT PRIMARY KEY,
    n_scenarios INT,
    n_anthills INT,
    n_ants_searching_food INT,
    n_ants INT,
    foods_in_anthills INT,
    foods_in_deposit INT,
    foods_in_transit INT, 
    foods_total INT, 
    avg_execution_time INT,
    fst_scenario_id VARCHAR(256),
    fst_scenario_time INT,
    slw_scenario_id VARCHAR(256),
    slw_scenario_time INT,
    avg_ant_food FLOAT,
    max_ant_food INT,
    CONSTRAINT fk_fst_scenario
        FOREIGN KEY(fst_scenario_id)
            REFERENCES scenarios(scenario_id),
    CONSTRAINT fk_slw_scenario
        FOREIGN KEY(slw_scenario_id)
            REFERENCES scenarios(scenario_id)
);"""

# Create table for local statistics 
DB_CREATE_LOCAL = """CREATE TABLE IF NOT EXISTS stats_local ( 
    scenario_id VARCHAR(256) PRIMARY KEY, 
    n_anthills INT, 
    n_ants INT, 
    n_foods INT, 
    execution_time INT, 
    active INT, 
    CONSTRAINT fk_scenario_id 
        FOREIGN KEY(scenario_id) 
            REFERENCES scenarios(scenario_id) 
);""" 

# Create table for atomic statistics 
DB_CREATE_ATOMIC = """CREATE TABLE IF NOT EXISTS stats_atomic ( 
    scenario_id VARCHAR(256), 
    anthill_id VARCHAR(256), 
    n_ants INT, 
    n_ants_searching_food INT, 
    foods_in_anthills INT, 
    foods_in_transit INT, 
    probability FLOAT,  
    CONSTRAINT fk_scenario_id 
        FOREIGN KEY(scenario_id) 
            REFERENCES scenarios(scenario_id), 
    CONSTRAINT fk_anthill_id 
        FOREIGN KEY(anthill_id) 
            REFERENCES anthills(anthill_id), 
    PRIMARY KEY(scenario_id, anthill_id) 
);""" 

DB_CREATE_BENCHMARKS = """CREATE TABLE IF NOT EXISTS benchmarks ( 
    id SERIAL PRIMARY KEY, 
    scenario_id VARCHAR(256), 
    computed_at TIMESTAMPTZ DEFAULT Now(), 
    CONSTRAINT fk_scenario_id 
        FOREIGN KEY(scenario_id) 
            REFERENCES scenarios(scenario_id) 
);""" 

# Database attributes 
HOST = "database-postgres-tjg.cvb1csfwepbn.us-east-1.rds.amazonaws.com" 
USER = "postgres" 
PASSWORD = "passwordpassword" 

def init_db( 
        host: str, 
        user: str, 
        password: str, 
        database: str, 
        drop_tables: bool=False
    ): 
    """ 
    Initialize the data base. 
    """ 
    db_conn = psycopg2.connect( 
            host=host, 
            user=user,
            password=password, 
            database=database,
    ) 
    
    db_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 

    # Instantiate a cursor 
    cursor = db_conn.cursor() 
    
    # and execute the DDL queries 
    if not drop_tables: 
        try: 
            cursor.execute(DB_CREATE) 
        except psycopg2.errors.DuplicateDatabase: 
            print("The database {db_name} already exists!" \
                    .format(db_name=DATABASE)) 
        cursor.execute(DB_CREATE_SCENARIOS) 
        cursor.execute(DB_CREATE_ANTHILLS) 
        cursor.execute(DB_CREATE_ANTS) 
        cursor.execute(DB_CREATE_FOODS) 
        cursor.execute(DB_CREATE_GLOBAL) 
        cursor.execute(DB_CREATE_LOCAL) 
        cursor.execute(DB_CREATE_ATOMIC) 
        cursor.execute(DB_CREATE_BENCHMARKS) 
    else: 
        # Drop all tables 
        cursor.execute(DROP_TABLES) 

    cursor.connection.commit() 
    
    cursor.close() 
    db_conn.close() 

if __name__ == "__main__": 
    # Capture the command line parameters 
    args = sys.argv 
    
    # Whether to drop the tables 
    drop_tables = (len(args) > 1) and (int(args[1]) == 1) 
     
    print("Use\npython operational_db.py [drop_tables]") 

    init_db( 
            host=HOST, 
            user=USER, 
            password=PASSWORD, 
            database=DATABASE, 
            drop_tables=drop_tables
    ) 
    print("Database {action}!".format( 
        action="purged" if drop_tables else "initialized") 
    ) 
