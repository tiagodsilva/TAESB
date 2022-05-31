""" 
Implement queries for the database. 
""" 
DB_CREATE_QUERY = """CREATE TABLE IF NOT EXISTS anthills ( 
    anthill_id serial PRIMARY KEY, 
    name VARCHAR (99) NOT NULL, 
    food_storage INT 
);""" 
