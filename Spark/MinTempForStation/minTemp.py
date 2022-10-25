# find the minimum temperature for each station id

from pyspark import SparkContext

#Function to get the minimum
def minimum(x, y):
    if x > y:
        return y
    else:
        return x


sc = SparkContext("local[*]", "min_temp")
sc.setLogLevel("ERROR")
input_data = sc.textFile("/Users/gupta/Downloads/datasets/tempdata.csv")

mappedInput = input_data.map(lambda x: (x.split(",")[0],
                                        float(x.split(",")[3])))

minTemp = mappedInput.reduceByKey(lambda x, y: minimum(x, y))

result = minTemp.collect()

#displaying the results
for res in result:
    station = res[0]
    mintemp = res[1]
    print(f"{station} minimum temperature: {mintemp} F")
