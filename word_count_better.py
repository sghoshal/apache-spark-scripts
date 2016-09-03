# Count all the words in the Book dataset and sort them by number of times each word occurs.

import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("dataset/Book.txt")
words = input.flatMap(normalizeWords)
# wordCount = words.countByValue()

# This is basically what countByValue() does internally.
# We are creating a key = word with value = 1 for each word obtained from the flatMap
# We are reducing by the key generated above(word) and adding up the values = 1s to get 
# the total count.

# We do this because we want wordCounts to be a rdd instead of a Python object.
# words.countByValue() returns a python object.
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y : (x + y))

# We need to flip the key value pair so that we can sort by the count (key)
wordCountsSorted = wordCounts.map(lambda (x, y): (y, x)).sortByKey(False)

results = wordCountsSorted.collect()

for result in results:
	count = result[0]
	cleanWord = result[1].encode('ascii', 'ignore')
	if (cleanWord):
		print cleanWord, count