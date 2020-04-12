from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("./data/super/marvel-names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("./data/super/marvel-graph.txt")

# Note how lambda functions in .reduceByKey() are passed two parameters
# while lambda functions in .map() are only passed one
pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y: x + y)
flipped = totalFriendsByCharacter.map(lambda x: (x[1],x[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(mostPopularName.decode('utf-8') + " is the most popular super, with " +
        str(mostPopular[0]) + " co-appearances.")
