# Some SQL stuff with spark.

from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

# Create a SparkSession 
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///D:/temp").appName("SparkSQL").getOrCreate()

def peopleMapper(line):
    tokens = line.split(",")
    return Row(ID=int(tokens[0]), name=tokens[1].encode('utf-8'), age=int(tokens[2]), numFriends=int(tokens[3]))

lines = spark.sparkContext.textFile("dataset/fakefriends.csv")
people = lines.map(peopleMapper)

# Infer the schema from the DataFrame
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

print "Teenagers: "
for teen in teenagers.collect():
    print teen

# Use some functions. This does the printing as well.
schemaPeople.groupBy("age").count().orderBy("age").show()

