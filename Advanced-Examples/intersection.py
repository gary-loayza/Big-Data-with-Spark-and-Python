from pyspark import SparkConf, SparkContext

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

conf = SparkConf().setMaster("local[*]").setAppName("intersection test")
sc = SparkContext(conf = conf)

data = sc.textFile("./data/ml-100k/u.data")
item = sc.textFile("./data/ml-100k/u.item")

ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
genres = item.map(lambda x: x.split("|")).map(
        lambda x: (int(x[0]), list(map(int, x[5:]))))

joinedRatings = ratings.join(ratings)
joinedGenres = genres.cartesian(genres)

uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

relevantJoinedGenres = joinedGenres.filter(filterRelevants)

genreCheckList = relevantJoinedGenres.map(makeCheckList)

checkList = sc.broadcast(genreCheckList.collect())

moviePairs = uniqueJoinedRatings.map(makePairs)

print("Number of Movie Pairs: ", moviePairs.count())
print("Number of movie combinations with matching genres: ", genreCheckList.count())

# This results in an empty RDD
# how can I pass in moviePairs keys only into the intersection method?
#moviePairs = moviePairs.intersection(genreCheckList)

#Try using filter function
relevantMoviePairs = moviePairs.filter(lambda e: (e[0][0],e[0][1]) in
        checkList.value)

# The result below is precisely what I intended
print(relevantMoviePairs.take(10))

# How large is relevantMoviePairs RDD?
#
# The line below does not run after 3 hours on my system
# Comment out for now
#print(relevantMoviePairs.count())
