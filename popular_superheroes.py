from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularSuperHeroes")
sc = SparkContext(conf = conf)

def parseLine(line):
	tokens = line.rstrip().split(' ')
	return (int(tokens[0]), len(tokens) - 1)

def loadHeroNames():
	marvelNames = {}
	with open("dataset/Marvel-Names.txt") as f:
		for line in f:
			tokens = line.split('"')
			marvelNames[int(tokens[0].strip())] = tokens[1].strip()

	return marvelNames

lines = sc.textFile("dataset/Marvel-Graph.txt")

# Load the hero ID to names mapping as RDD.
marvelNamesDict = sc.broadcast(loadHeroNames())

# Get the tuple of hero ID to co hero count per line.
parsedLines = lines.map(parseLine)

# Hero To Costars total count (reduced by hero ID)
heroToCoStars = parsedLines.reduceByKey(lambda x, y: (x + y))

flipped = heroToCoStars.map(lambda (x, y): (y, x))
mostPopular = flipped.max()
mostPopularName = marvelNamesDict.value[mostPopular[1]]

print mostPopularName, "is the most popular super hero with ", mostPopular[0], "appearances!"