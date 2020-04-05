from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# Function for parsing each string/row from raw data
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# Read data from file and apply parsing function
lines = sc.textFile("./data/fakefriends.csv")
rdd = lines.map(parseLine)

# Modify RDD by appending a 'counter' to the value as a tuple
# This will result in the following data structure for example:
#   (33, (180, 1))
#   (33, (207, 1))
# Then, add the number of friends as well as the 'counter' for that key
# This will result in the following data structure for example:
#   (33, (387, 2))
totalsByAge = rdd.mapValues(
    lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Average the new tuples of (totals, counts) and divide totals over counts
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# Collect and print results
results = averagesByAge.collect()
for result in results:
    print(result)
