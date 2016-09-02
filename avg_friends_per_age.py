from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

def parse_line(line):
	tokens = line.split(',')
	age = int(tokens[2])
	friends = int(tokens[3])

	return (age, friends)

lines = sc.textFile("dataset/fakefriends.csv")
rdd = lines.map(parse_line)

# 25,1411
# 23 1890
# 25 108
# 63 211

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y [1]))
avgByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

results = avgByAge.collect()

for result in results:
	print result

