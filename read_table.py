from pyspark.sql import SparkSession 

spark = SparkSession \
        .builder \
        .appName("Python Spark SQL") \
        .config("spark.jars", "postgresql-42.3.6.jar") \
        .getOrCreate() 

df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "ants") \
        .option("user", "tiago") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()

if __name__ == "__main__": 
    print(df.printSchema()) 
