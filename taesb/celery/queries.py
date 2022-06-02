""" 
Implement queries for the database. 
""" 
# Create table for the scenarios 
DB_CREATE_SCENARIOS = """CREATE TABLE IF NOT EXISTS scenarios ( 
    scenario_id INT PRIMARY KEY, 
    execution_time INT DEFAULT NULL
);""" 

# Create table for the anthills  
DB_CREATE_ANTHILLS = """CREATE TABLE IF NOT EXISTS anthills ( 
    anthill_id INT PRIMARY KEY,
    name VARCHAR (99) NOT NULL, 
    food_storage INT, 
    scenario_id INT, 
    CONSTRAINT fk_scenario 
        FOREIGN KEY(scenario_id) 
            REFERENCES scenarios(scenario_id) 
);""" 

# Create table for the ants 
DB_CREATE_ANTS = """CREATE TABLE IF NOT EXISTS ants ( 
    ant_id INT PRIMARY KEY, 
    captured_food INT, 
    searching_food BOOL, 
    scenario_id INT, 
    CONSTRAINT fk_scenario 
        FOREIGN KEY(scenario_id) 
            REFERENCES scenarios(scenario_id) 
);""" 

# Create table for the foods 
DB_CREATE_FOODS = """CREATE TABLE IF NOT EXISTS foods ( 
    food_id INT PRIMARY KEY, 
    initial_volume INT, 
    current_volume INT, 
    scenario_id INT, 
    CONSTRAINT fk_scenario 
        FOREIGN KEY(scenario_id)  
            REFERENCES scenarios(scenario_id) 
);""" 



