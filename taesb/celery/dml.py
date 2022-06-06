""" 
Implement queries for the database. 
""" 
from typing import List, Dict 

# Insert data into scenarios table 
INSERT_SCENARIOS = lambda scenario_id, execution_time, active: """INSERT INTO scenarios  (scenario_id, execution_time, active) VALUES 
    ('{scenario_id}', {execution_time}, {active}) 
ON CONFLICT (scenario_id) 
DO 
    UPDATE SET execution_time = {execution_time}, 
               active = {active};""".format(
            scenario_id=scenario_id, 
            execution_time=execution_time, 
            active=active) 

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
        food_storage = anthill["food_storage"] 
        query += "INSERT INTO anthills VALUES \n" 
        # Insert into data base if it not exists; else, update the value 
        query += "('{anthill_id}', '{name}', {food_storage}, {total_ants}, '{scenario_id}')\n".format( 
                anthill_id=anthill_id, 
                name=name,
                food_storage=food_storage, 
                total_ants=total_ants, 
                scenario_id=scenario_id) 
        # Check if a register already exists; if so, update it 
        query += """ON CONFLICT (anthill_id) 
    DO 
        UPDATE SET food_storage = {food_storage};\n""".format( 
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
        ant_id = ant["identifier"] 
        captured_food = ant["captured_food"] 
        anthill_id = ant["anthill_identifier"] 
        searching_food = not ant["has_food"] 

        query += "INSERT INTO ants VALUES \n" 
        # Insert the instances 
        query += "('{ant_id}', {captured_food}, {searching_food}, '{anthill_id}') \n".format( 
                ant_id=ant_id, 
                captured_food=captured_food,
                searching_food=int(searching_food),
                anthill_id=anthill_id 
        ) 
        # Update data if it already exists 
        query += """ON CONFLICT (ant_id) 
DO 
    UPDATE SET captured_food = {captured_food}, 
               searching_food = {searching_food};\n""".format( 
            captured_food=captured_food, 
            searching_food=int(searching_food), 
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
        query += "('{food_id}', {initial_volume}, {current_volume}, '{scenario_id}')".format( 
                food_id=food_id, 
                initial_volume=initial_volume, 
                current_volume=current_volume, 
                scenario_id=scenario_id
        ) 

        # Update data if it already exists 
        query += """ON CONFLICT (food_id) 
DO 
    UPDATE SET current_volume = {current_volume};\n""".format( 
            food_id=food_id, 
            current_volume=current_volume) 

    # Return the query 
    return query 

# Capture data 
def CAPTURE_DATA(): 
    """ 
    Join the tables at the data set and consolidate the data, which could be 
    used as a RDD with PySpark. 
    """ 
    # Identify the ants (in PySpark, queries do not use `;`) 
    queries = ["SELECT * FROM ants", 
            "SELECT * FROM anthills", 
            "SELECT * FROM scenarios", 
            "SELECT * FROM foods" 
    ] 
    # Return the queries 
    return queries 

