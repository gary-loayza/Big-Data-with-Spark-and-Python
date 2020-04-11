from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("../data/ml-100k/u.item", encoding = "ISO-8859-1") as f:
        # u.item data columns
        # Movie ID | Movie Name | Release Date | . . .
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

# In the previous script, we were displaying Movie IDs.
# Using ../data/ml-100k/u.item
# we can match the Movie ID to a Movie Name.
#
# Spark can transfer any object to all your executor nodes efficiently.
# Use sc.broadcast() combined with .value() to retrieve value
nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("./data/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda x: (x[1], x[0]) )
sortedMovies = flipped.sortByKey()

# Since we're more concerned about formmatting in this script,
# we can take the flip the count column back to the right and
# add the Movie Name column by calling .value() on nameDict
sortedMoviesWithNames = sortedMovies.map(lambda x: (nameDict.value[x[1]], x[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)
