# Goal: To add up amount spent by customer from customer-orders.csv
# Note that the data is organized in the following format:
# Customer ID | Item Number | Amount Spent

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AmountByCustomer")
sc = SparkContext(conf = conf)

# Function for parsing each row
def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

lines = sc.textFile("./data/customer-orders.csv")
rdd = lines.map(parseLine)

# Sum the amounts that are of the same key.
# x and y must be arbitrary but different values for the same key.
amountCount = rdd.reduceByKey(lambda x, y: x + y)

# To sort by the amount spent, we need to reverse the value to be the key, so
# that it can sort by the amount. In this case x[1] must come first.
amountSorted = amountCount.map(lambda x: (x[1],x[0])).sortByKey()

results = amountSorted.collect();
for result in results:
    customer = str(result[1])
    amount = str(result[0])
    if (customer):
        print("Customer " + customer + ":\t" + amount)
