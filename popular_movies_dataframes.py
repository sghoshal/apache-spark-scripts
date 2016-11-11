# Find out popular movies (most viewed) using Dataframes (Spark SQL)

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///D:/temp").appName("SparkSQL").getOrCreate()

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            tokens = line.split("|")
            movieNames[int(tokens[0])] = tokens[1]

    return movieNames

nameDict = loadMovieNames()
lines = spark.sparkContext.textFile("ml-100k/u.data")
movies = lines.map(lambda x : Row(movieID = int(x.split()[1])))
movieDataset = spark.createDataFrame(movies)

topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False).cache()

topMovieIDs.show()

top10 = topMovieIDs.take(10)

print "\n"

for topMovie in top10:
    print nameDict[int(topMovie[0])], topMovie[1]