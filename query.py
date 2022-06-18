""" 
Query the database. 
""" 
# Import the application 
import taesb 

# Access to the database 
import psycopg2 
import pandas as pd 

# Docs 
import argparse 
from typing import List 

def parse_args(): 
    """ 
    Parse the command line parameters. 
    """ 
    parser = argparse.ArgumentParser(description="Queries in the database.") 
    
    # Insert parameters for the command line 
    parser.add_argument("--query", help="Query to the database (with SQL format).", 
            type=str) 

    args = parser.parser_args() 
    
    # Return the parsed args 
    return args 

def conn(): 
    """ 
    Access the database. 
    """ 
    db_conn = pycopg2.connect( 
            host=os.environ["POSTGRESQL_HOST"], 
            database=os.environ["POSTGRESQL_DATABASE"], 
            user=os.environ["POSTGRESQL_USER"], 
            password=os.environ["POSTGRESQL_PASSWORD"] 
    ) 

    # Return the access to the data base 
    return db_conn 

def main(): 
    """ 
    Execute the user's query. 
    """ 
    # Access the data base 
    db_conn = conn() 
    # Capture user's parameters 
    args = parse_args() 
    query = args.query 

    # Execute the query 
    cursor = db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) 
    cursor.execute(query) 

    values = cursor.fetchall() 
    cursor.close() 

    # Instantiate a data frame with the data 
    dataframe = pd.DataFrame(values) 

    print(tabulate(dataframe, header="keys", 
        tablefmt="psql")) 

if __name__ == "__main__": 
    main() 

