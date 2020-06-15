import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
    movieNames = {}
    with open("./data/ml-100k/u.item", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

#Python 3 doesn't let you pass around unpacked tuples,
#so we explicitly extract the ratings now.
def makePairs( userRatings ):
    """
    Extracts a movie and rating from the RDD, and outputs a new RDD where the
    key is the movie pair, and the value are the respective ratings for that
    movie pair.
    """
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def makeCheckList( relevantMovies ):
    """
    Removes the genre list from the RDD, transforming from
    ((movieID, [genre1, ..., genre15]), (movieID, [genre1, ..., genre15]))
    to
    (movieID, movieID)
    """
    movie1 = relevantMovies[0][0]
    movie2 = relevantMovies[1][0]
    return (movie1, movie2)

def filterDuplicates( userRatings ):
    """
    .filter() Documentation:
        Return a new dataset formed by selecting those elements of
        the source on which func returns true.

    In this case, return only the pair of movies where the movie with the
    higher movieID is listed second. In reality, this inequality could face any
    direction. The important part is to include a single movie pair only once.
    """
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

def filterRelevants(genreCombos):
    """
    Check each genre and determine if there is a match.
    Also filter out any combinations of the same movie.
    """
    (movie1, genre1) = genreCombos[0]
    (movie2, genre2) = genreCombos[1]

    if (movie1 == movie2):
        return False
    elif genre1[0] and genre2[0]:
        return True
    elif genre1[1] and genre2[1]:
        return True
    elif genre1[2] and genre2[2]:
        return True
    elif genre1[3] and genre2[3]:
        return True
    elif genre1[4] and genre2[4]:
        return True
    elif genre1[5] and genre2[5]:
        return True
    elif genre1[6] and genre2[6]:
        return True
    elif genre1[7] and genre2[7]:
        return True
    elif genre1[8] and genre2[8]:
        return True
    elif genre1[9] and genre2[9]:
        return True
    elif genre1[10] and genre2[10]:
        return True
    elif genre1[11] and genre2[11]:
        return True
    elif genre1[12] and genre2[12]:
        return True
    elif genre1[13] and genre2[13]:
        return True
    elif genre1[14] and genre2[14]:
        return True
    else:
        return False

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)


conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf = conf)

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("./data/ml-100k/u.data")
item = sc.textFile("./data/ml-100k/u.item")

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
# Map genres to key / value pairs: (movieID, [genre1, genre2, ..., genre15])
genres = item.map(lambda x: x.split("|")).map(
        lambda x: (int(x[0]), list(map(int, x[5:]))))

# Emit every movie rated together by the same user.
# Self-join to find every combination.
joinedRatings = ratings.join(ratings)

# Emit every movie combination with their respective genres attached as a list
joinedGenres = genres.cartesian(genres)

# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))
# The joinedGenres RDD consists of:
# ((movieID, [genre1, ..., genre15]), (movieID, [genre1, ..., genre15]))

# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Filter out duplicate genres and movies with no relevance
relevantJoinedGenres = joinedGenres.filter(filterRelevants)

# Now, in order to compile the relevants from the resulting filter into an
# applicable list, we must map() the genres RDD into the movie pair form.
genreCheckList = relevantJoinedGenres.map(makeCheckList)

checkList = sc.broadcast(genreCheckList.collect())

# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatings.map(makePairs)

# From here, we can use the list as a reference for a filter by performing
# some check to see if the final results of the moviePairs RDD has some entry
# in the genreCheckList.
relevantMoviePairs = moviePairs.filter(lambda e: (e[0][0],e[0][1]) in
        checkList.value)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = relevantMoviePairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

# Save the results if desired
#moviePairSimilarities.sortByKey()
#moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
