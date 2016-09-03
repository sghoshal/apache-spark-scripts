# Find the min temperature by location

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTempByLocation")
sc = SparkContext(conf = conf)

def parseLine(line):
	tokens = line.split(',')

	location = tokens[0]
	entryType = tokens[2]
	temperature = float(tokens[3]) * 0.1 * (9.0 / 5.0) + 32.0

	return (location, entryType, temperature)

# Load the text file
lines = sc.textFile("dataset/1800.csv")

# Parse each line using the parseLine method to convert it to a tuple.
parsedLines = lines.map(lambda line: parseLine(line))

# Filter those lines that have "TMIN" in the (location, entry, temperature) tuple.
minTempLines = parsedLines.filter(lambda parsedLine: "TMIN" in parsedLine)

# From the filtered (minTemps) rdd, get only location and entryType.
stationTemps = minTempLines.map(lambda x: (x[0], x[2]))		

# Get the min of temperatures when grouping/reducing by the same key 
# minTemps = rddParsed.reduceByKey(lambda x, y: min(x[2], y[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))

results = minTemps.collect()

for result in results:
	print result[0] + "\t{:.2f}F".format(result[1])


