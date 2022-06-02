""" 
Implement queries for the database. 
""" 
from typing import List 

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

# Insert data into scenarios table 
INSERT_SCENARIOS = lambda scenario_id: """INSERT INTO scenarios VALUES 
    ({scenario_id})""".format(scenario_id=scenario_id) 

def INSERT_ANTHILLS( 
            anthill_ids: List[str], 
            names: List[str], 
            food_storages: List[int], 
            scenario_ids: List[int] 
        ): 
    """ 
    Insert instance into the anthills table. 
    """ 
    # Initialize query 
    query = str() 
    # Iterate across the attributes 
    for (anthill_id, name, food_storage, scenario_id) in zip( 
            anthill_ids, names, food_storages, scenario_ids): 
        query += "INSERT INTO anthills VALUES \n" 
        # Insert into data base if it not exists; else, update the value 
        query += "({anthill_id}, {name}, {food_storage}, {scenario_id})\n".format( 
                anthill_id=anthill_id, 
                name=name,
                food_storage=food_storage,
                scenario_id=scenario_id) 
        # Check if a register already exists; if so, update it 
        query += """ON CONFLICT ({anthill_id}) 
    DO 
        UPDATE SET food_storage = {food_storage} 
        WHERE anthill_id = {anthill_id};\n""".format( 
                    food_storage=food_storage, 
                    anthill_id=anthill_id) 
    
    # Return the query 
    return query 

def INSERT_ANTS( 
        ant_ids: List[str], 
        captured_foods: List[int], 
        searching_food: List[bool], 
        scenario_id: List[int] 
    ): 
    """ 
    Insert data into the ants table. 
    """ 
    # Initialize the query 
    query = str() 
    # Iterate across the attributes 
    for (ant_id, captured_food, searching_food, scenario_id) in zip( 
            ant_ids, captured_foods, searchgin_foods, scanerio_ids): 
        query += "INSERT INTO ants VALUES \n" 
        # Insert the instances 
        query += "({ant_id}, {captured_food}, {searching_food}, {scenario_id}) \n".format( 
                ant_id=ant_id, 
                captured_food=captured_food,
                searching_food=searching_food,
                scenario_id=scenario_id 
        ) 
        # Update data if it already exists 
        query += """CONSTRAINT ({ant_id}) 
DO 
    UPDATE SET captured_food = {captured_food}, 
               searching_food = {searching_food} 
    WHERE ant_id = {ant_id};\n""".format( 
            captured_food=captured_food, 
            searching_food=searching_food, 
            ant_id=ant_id) 

    # Return the query 
    return query 
