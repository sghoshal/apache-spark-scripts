# Get most movies to watch count sorted.

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerExpenditure")
sc = SparkContext(conf = conf)

movies = sc.textFile("ml-100k/u.data")

# Split on white space and convert each entry into (movieID, 1) tuples.
movieTuples = movies.map(lambda movie: (int(movie.split()[1]), 1))

# Reduce by movie ID (key) and sum up the values (count = 1)s.
movieByCount = movieTuples.reduceByKey(lambda x, y: (x + y))

# Flip the key, value to sort by watched count.
flipped = movieByCount.map(lambda (x, y): (y, x))

# Sort by key (= watched count).
sortedByCount = flipped.sortByKey()

results = sortedByCount.collect()

for result in results:
	print "Movie: ", result[1], "Count: ", result[0]