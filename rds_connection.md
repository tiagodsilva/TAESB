---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.13.8
  kernelspec:
    display_name: Python 3.8.8 ('base')
    language: python
    name: python3
---

## RDS Connection

```python
# importing DDL queries to create the tables
from taesb.celery.operational_db import DB_CREATE_SCENARIOS, \
                                        DB_CREATE_ANTHILLS, \
                                        DB_CREATE_ANTS, \
                                        DB_CREATE_FOODS, \
                                        DB_CREATE_GLOBAL, \
                                        DB_CREATE_LOCAL, \
                                        DB_CREATE_ATOMIC, \
                                        DROP_TABLES 

# importing DML queries to fill the tables
# TODO: update the current code to fill the table in AWS RDS instead the local database
from taesb.celery.dml import INSERT_ANTS, \
        INSERT_ANTHILLS, \
        INSERT_FOODS, \
        INSERT_SCENARIOS                                     
```

#### We will (try to) use PyGreSQL

To use `import pgbd`, first run `pip pygresql`

```python
!pip install pygresql
```

Trying to use `pgdb`, but a unresolved problem disabled its use.

```python
# Using PyGreSQL (pgdb)
import pgdb

db = pgdb.connect(
        host = "database-postgres-tjg.cvb1csfwepbn.us-east-1.rds.amazonaws.com",
        user="postgres",
        password="passwordpassword",
        database="database-postgres-tjg")
```

Now, we are going to use `psycopg2` to make the connection with the AWS

```python
# importing
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# creating a connection
db = psycopg2.connect(
        host = "database-postgres-tjg.cvb1csfwepbn.us-east-1.rds.amazonaws.com",
        user="postgres",
        password="passwordpassword",
        database="operational_tjg" #database previously created
        )

# setting the isolation level
db.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
```

> Obs.: the field `database` is already set because it was previously created in python. To know how it was made, take a look at [Creating `operational_tjg` database](#creating-operational_tjg-database)

```python
# initializing cursor
cursor = db.cursor()
```

```python
# check version
cursor.execute("SELECT version()")
cursor.fetchall()
```

```python
# list tables
show_tables_query = """
SELECT relname 
FROM pg_class 
WHERE relkind='r' AND relname !~ '^(pg_|sql_)';
"""
cursor.execute(show_tables_query)

cursor.fetchall()
```

### Creating tables

```python
cursor.execute(DB_CREATE_SCENARIOS)
cursor.execute(DB_CREATE_ANTHILLS)
cursor.execute(DB_CREATE_ANTS)
cursor.execute(DB_CREATE_FOODS)
cursor.execute(DB_CREATE_GLOBAL)
cursor.execute(DB_CREATE_LOCAL)
cursor.execute(DB_CREATE_ATOMIC)

cursor.connection.commit()
```

```python
cursor.execute(show_tables_query)

cursor.fetchall()
```

```python
# check `scenarios` structure
cursor.execute("""
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'scenarios';
""")

cursor.fetchall()
```

```python
# closing connection
cursor.close()
```

### Inserting data


To insert the data, a update in the current code in `task.py` is needed.


---
### Appendix 
---
---



#### Creating `operational_tjg` database


```python
# creating a connection
db = psycopg2.connect(
        host = "database-postgres-tjg.cvb1csfwepbn.us-east-1.rds.amazonaws.com",
        user="postgres",
        password="passwordpassword"
        )

# setting the isolation level
db.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
```

```python
# initializing cursor
cursor = db.cursor()
```

```python
# creating database
cursor.execute("CREATE DATABASE operational_tjg")

# committing changes
cursor.connection.commit()

# closing connection
cursor.close()
```
