import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCountRegEx")
sc = SparkContext(conf = conf)

input = sc.textFile("./data/book.txt")
words = input.flatMap(normalizeWords)

# using .countByValue() works well,
#       wordCounts = words.countByValue()
# but results in a python object. Instead of sorting from a python object
# it is better to maintain the data in a RDD. In that case, we will need to
# continue using map/reduce methods.
wordCounts = words.map(lambda x:(x,1)).reduceByKey(lambda x, y: x + y)
# and now we can sort by the counts.
wordCountsSorted = wordCounts.map(lambda x: (x[1],x[0])).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1]
    if (word):
        print(word + ":\t\t\t" + count)
