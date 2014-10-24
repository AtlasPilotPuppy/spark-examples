from pyspark import SparkContext

sc = SparkContext('spark://master:7077', 'accumulator example')
# accumulators are initialized with a initial value
# they have and add method to add values to the accumulator
# and a value property that is visibile only to the master

accum = sc.accumulator(0)
data = sc.parallelize(range(1,1000))

# we are going to iterate over our data and add each value to the 
# accumulator

data.foreach(lambda value: accum.add(value))

print accum.value
