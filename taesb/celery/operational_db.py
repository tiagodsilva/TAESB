""" 
Consolidate the DDL queries for the operational database.  
""" 
# Drop tables; appropriate for debugging 
DROP_TABLES = """DROP TABLE IF EXISTS scenarios CASCADE; 
DROP TABLE IF EXISTS anthills CASCADE; 
DROP TABLE IF EXISTS ants CASCADE; 
DROP TABLE IF EXISTS foods CASCADE; 
DROP TABLE IF EXISTS stats CASCADE;""" 

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

# Create tabke for the statistics
DB_CREATE_STATS = """CREATE TABLE IF NOT EXISTS stats (
    stat_id INT PRIMARY KEY,
    n_scenarios INT,
    n_anthills INT,
    n_ants_searching_food INT,
    n_ants INT,
    n_food_anthills INT,
    n_food INT,
    execution_time INT,
    fst_scenario_id VARCHAR(256),
    fst_scenario_time INT,
    slw_scenario_id VARCHAR(256),
    slw_scenario_time INT,
    avg_ant_food INT,
    max_ant_food INT,
    CONSTRAINT fk_fst_scenario
        FOREIGN KEY(fst_scenario_id)
            REFERENCES scenarios(scenario_id),
    CONSTRAINT fk_slw_scenario
        FOREIGN KEY(slw_scenario_id)
            REFERENCES scenarios(scenario_id)
)"""

