# Get most movies to watch count sorted.

from pyspark import SparkConf, SparkContext

def loadMovieNames():
	movieNames = {}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1]

	return movieNames

conf = SparkConf().setMaster("local").setAppName("CustomerExpenditure")
sc = SparkContext(conf = conf)

# Broadcast the returned movieNames from the method to all the nodes in the cluster.
nameDict = sc.broadcast(loadMovieNames())

movies = sc.textFile("ml-100k/u.data")

# Split on white space and convert each entry into (movieID, 1) tuples.
movieTuples = movies.map(lambda movie: (int(movie.split()[1]), 1))

# Reduce by movie ID (key) and sum up the values (count = 1)s.
movieByCount = movieTuples.reduceByKey(lambda x, y: (x + y))

# Flip the key, value to sort by watched count.
flipped = movieByCount.map(lambda (x, y): (y, x))

# Sort by key (= watched count).
sortedByCount = flipped.sortByKey()

# Use the nameDict broadcast variable to get the name for the movieID.
sortedMoviesWithNames = sortedByCount.map(lambda (count, movieID): (nameDict.value[movieID], count))
results = sortedMoviesWithNames.collect()

for result in results:
	print "Movie: ", result[0], "Count: ", result[1]