# List total expenses per customer sorted by total expenditure per customer in descending order
# customer-orders.csv has entries of format: customerID,productID,expense

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerExpenditure")
sc = SparkContext(conf = conf)

# If the keys are casted to integer, the result will be sorted by default.
# If the keys are kept as string, its not sorted.

# Reason:
# Spark behaves like MapReduce in this aspect. Could come in handy! 
# Bear in mind it's probably only sorting within a given reducer node though; 
# if you're running on a cluster, the final results may be split up.

def parseLine(entry):
	tokens = entry.split(',')
	customerID = int(tokens[0])
	expense = float(tokens[2])

	return (customerID, expense)

lines = sc.textFile("dataset/customer-orders.csv")
customerExpenses = lines.map(parseLine)
groupedByCustomer = customerExpenses.reduceByKey(lambda x, y: (x + y))
sortedByTotalExpense = groupedByCustomer.map(lambda (x, y): (y, x)).sortByKey(False)

results = sortedByTotalExpense.collect()

for result in results:
	print "Customer: ", result[1], ", Total: ", result[0]
