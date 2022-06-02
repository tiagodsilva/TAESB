""" 
Implement queries for the database. 
""" 
from typing import List, Dict 

# Drop tables; appropriate for debugging 
DROP_TABLES = """DROP TABLE IF EXISTS scenarios CASCADE; 
DROP TABLE IF EXISTS anthills CASCADE; 
DROP TABLE IF EXISTS ants CASCADE; 
DROP TABLE IF EXISTS foods CASCADE;""" 

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
    total_ants INT, 
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
    anthill_id INT, 
    CONSTRAINT fk_anthill 
        FOREIGN KEY(anthill_id) 
            REFERENCES anthills(anthill_id) 
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
            anthills: List[Dict], 
            scenario_id: str 
        ): 
    """ 
    Insert instance into the anthills table. 
    """ 
    # Initialize query 
    query = str() 
    # Iterate across the attributes 
    for anthill in anthills: 
        # Capture attributes 
        anthill_id = anthill["identifier"] 
        name = anthill["name"] 
        total_ants = anthill["ants"] 
        query += "INSERT INTO anthills VALUES \n" 
        # Insert into data base if it not exists; else, update the value 
        query += "({anthill_id}, {name}, {food_storage}, {total_ants}, {scenario_id})\n".format( 
                anthill_id=anthill_id, 
                name=name,
                food_storage=food_storage, 
                total_ants=total_ants, 
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
        ants: List[Dict], 
    ): 
    """ 
    Insert data into the ants table. 
    """ 
    # Initialize the query 
    query = str() 
    # Iterate across the attributes 
    for ant in ants: 
        and_id = ant["identifier"] 
        captured_food = ant["captured_food"] 
        anthill_id = ant["anthill_identifier"] 
        searching_food = not ant["has_food"] 

        query += "INSERT INTO ants VALUES \n" 
        # Insert the instances 
        query += "({ant_id}, {captured_food}, {searching_food}, {anthill_id}) \n".format( 
                ant_id=ant_id, 
                captured_food=captured_food,
                searching_food=searching_food,
                anthill_id=anthill_id 
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

# Insert the foods into the food data set 
def INSERT_FOODS( 
        foods: List[Dict], 
        scenario_id: str 
    ): 
    """ 
    Insert foods in the food data set table. 
    """ 
    # Initialize the query 
    query = str() 
    # Iterate across the attributes 
    for food in foods: 
        # Insert instances 
        food_id = food["identifier"] 
        initial_volume = food["initial_volume"] 
        current_volume = food["current_volume"] 

        query += "INSERT INTO foods VALUES \n" 
        query += "({food_id}, {initial_volume}, {current_volume}, {scenario_id})".format( 
                food_id=food_id, 
                initial_volume=initial_volume, 
                current_volume=current_volume, 
                scecnario_id=scenario_id
        ) 

        # Update data if it already exists 
        query += """CONSTRAINT ({food_id}) 
DO 
    UPDATE SET current_volume = {current_volume}, 
    WHERE food_id = {food_id};\n""".format( 
            food_id=food_id, 
            current_volume=current_volume) 

    # Return the query 
    return query 
