from pyspark import SparkContext

sc = SparkContext("local[*]", "movie-ratings")

sc.setLogLevel("ERROR")

input_data = sc.textFile("/Users/gupta/Downloads/datasets/moviedata.data")

mappedInput = input_data.map(lambda x: (x.split("\t")[2], 1))

reduced = mappedInput.reduceByKey(lambda x, y: x + y)

result = reduced.collect()

for i in result:
    print(i)
