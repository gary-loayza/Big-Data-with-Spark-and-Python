from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# Goal: We wish to determine the degree of separation between SpiderMan and
# Mavel character Adam 3,031
startCharacterID = 5306 #SpiderMan
targetCharacterID = 14  #Adam 3,031

# Our accumulator, used to signal when we find the target character during
# our Breadth First Seach (BFS) traversal.
hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    status = 'NEW'
    distance = 9999

    if (heroID == startCharacterID):
        status = 'PENDING'
        distance = 0

    return (heroID, (connections, distance, status))

def createStartingRdd():
    inputFile = sc.textFile('./data/super/marvel-graph.txt')
    return inputFile.map(convertToBFS)

def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    status = data[2]

    results = []

    # If this node needs to be expanded . . .
    if (status == 'PENDING'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newStatus = 'PENDING'
            if (targetCharacterID == connection):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newStatus))
            results.append(newEntry)

        # We've processed this node, so status is done
        status = 'DONE'

    # Emit the input node so we don't lose it.
    results.append( (characterID, (connections, distance, status)) )
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    status1 = data1[2]
    status2 = data2[2]

    distance = 9999
    status = 'NEW'
    edges = []

    # See if one is the original node with its connections.
    # If so, preserve them.
    if (len(edges1) > 0):
        edges = edges1
    elif (len(edges2) > 0):
        edges = edges2

    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1
    if (distance2 < distance):
        distance = distance2

    # Preserve most complete status
    if (status1 == 'NEW' and (status2 == 'PENDING' or status2 == 'DONE')):
        status = status2

    if (status2 == 'PENDING' and status2 == 'DONE'):
        status = status2

    return (edges, distance, status)

# Main program here:
iterationRdd = createStartingRdd()

for iteration in range(0, 10):
    print("Running BFS iteration #" + str(iteration+1))

    # Create new vertices as needed to progress status or reduce distance in
    # the reduce stage. If we encounter the node we're looking for as a PENDING
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMap)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target charater! From " + str(hitCounter.value)
                + " different direction(s)")
        break

    # Reducer combines data for each character ID, preserving the darkest color
    # and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)
