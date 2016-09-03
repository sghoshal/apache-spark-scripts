from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("dataset/Book.txt")
words = input.flatMap(lambda line : line.split())
wordCount = words.countByValue()

for word, count in wordCount.items():
	cleanWord = word.encode('ascii', 'ignore')
	if (cleanWord):
		print cleanWord, count