# Find the degree of separation of 2 character IDs using Breadth First Search.
# Dataset - marvel-graph.txt
# has the format: [heroID] followed by all the other heroIDs it has appeared with in comic books.
# Algorithm:
    # Create BFS nodes for characterID
    # Color the starting characterID = GRAY
    # Perform BFS from each of the GRAY node (initially just the starting characterID node)
    # Color the neighboring nodes = GRAY.
    # Create new nodes for each neighbor with color = GRAY and distance incremented by 1 (For flatMap operation)
    # Also make sure the original node with all the connection data is present in the flatMap result
    # Reduce all the nodes having the same key (heroID) into one with the shortest distance and darkest color

    # Perform these in a for loop till the target character ID is reached.
    
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

startCharacterID = 5306
targetCharactedID = 14

hitCounter = sc.accumulator(0)

def convertToBFSNodes(line):
    tokens = line.split()

    characterID = int(tokens[0])
    connectionsStr = tokens[1:]
    connections = []

    for conn in connectionsStr:
        connections.append(int(conn))

    color = 'WHITE'
    distance = 9999

    if (characterID == startCharacterID):
        color = 'GRAY'
        distance = 0

    return (characterID, (connections, distance, color))

def createInitialRDD():
    inputFile = sc.textFile("dataset/marvel-graph.txt")
    return inputFile.map(convertToBFSNodes)

def bfsExpand(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if color == 'GRAY':        
        for conn in connections:
            newDistance = distance + 1
            newColor = 'GRAY'

            if (targetCharactedID == conn):
                hitCounter.add(1)

            results.append((conn, ([], newDistance, newColor)))

        color = 'BLACK'
    
    results.append((characterID, (connections, distance, color)))

    return results

def bfsReduce(data1, data2):
    connections1 = data1[0]
    connections2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    connections = []
    distance = 9999
    color = 'WHITE'

    if len(connections1) > 0:
        connections.extend(connections1)

    if len(connections2) > 0:
        connections.extend(connections2)

    distance = min(distance1, distance2)

    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (connections, distance, color)

iterationRDD = createInitialRDD()

for i in range(0, 10):
    print "Interation i: ", i

    mapped = iterationRDD.flatMap(bfsExpand)
    print("Processing " + str(mapped.count()) + " values.")

    if hitCounter.value > 0:
        print "Hit the target character from ", hitCounter.value, "sides"
        break


    iterationRDD = mapped.reduceByKey(bfsReduce)