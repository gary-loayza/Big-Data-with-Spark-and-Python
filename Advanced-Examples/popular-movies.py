from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

# Column formatting:
# User ID | Movie ID | Rating | Timestamp
lines = sc.textFile("./data/ml-100k/u.data")

# Parse the RDD held in memory
movies = lines.map(lambda x: (int(x.split()[1]), 1))
# Count the movies with the same ID
movieCounts = movies.reduceByKey(lambda x, y: x + y)


# New structure:
# Count | Movie ID
#
# This is so that we can sort by the key,
# which Spark treats as the first column
flipped = movieCounts.map( lambda x: (x[1], x[0]) )
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)
