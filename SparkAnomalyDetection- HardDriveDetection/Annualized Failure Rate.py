
#With the help of my senior I was able to check my solution for 3b and hence submitting a jupyter notebook 
#with all output there, Since there is not much difference in code logic I am submitting in .py format

#[1]

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import max, count, col, avg


#[2]
spark = SparkSession\
        .builder\
        .appName("KMeansApp")\
        .getOrCreate()

spark


#[3]
dataset = spark.read.format("csv").option("header", "true").load("/Users/sabarnikundu/Downloads/failure_rate.csv")
dataset.show

#[4]
dataset = dataset.withColumn("annual_failure_rate", dataset["annual_failure_rate"].cast("float"))


#[5]
#Convert smart_1_normalized into a feature column and fetch it so that we can pass a vector input to KMeans method
Assembler = VectorAssembler(inputCols=["annual_failure_rate"], outputCol="features")
dataset = Assembler.setHandleInvalid("skip").transform(dataset).na.drop()

#[6]
mod = []
wssses = []
kval = [4,5,6,7]
for val in kval:
    
    print('Value of K = ', val)
    
    kmeans = KMeans().setK(val).setSeed(1)
    model = kmeans.fit(dataset)
    mod.append(model)

    # Evaluate clustering by computing Within Set Sum of Squared Errors.
    wssse = model.computeCost(dataset)
    wssses.append(wssse)
    print("Squared Errors = " + str(wssse))

    # Printing centers of clusters
    centers = model.clusterCenters()
    print("Centers of the clusters: ")
    for c in centers:
        print(c)


#[7]
model = mod[3]

#[8]
dataset.show()

#[9]
# Creating clusters using the selected model where k = 6 (Selected on the basis of observation in 3b) 
# Create with feature, cluster number and center for the predicted values
predictions = model.transform(dataset)
colnames = predictions.schema.names
centers = model.clusterCenters()
predictions = predictions.rdd.map(lambda x: tuple(list(x) + [int(centers[x[5]][0])] ))
predictions = predictions.toDF(colnames[:-1] + ['cluster', 'center'])


#[10]
predictions.show()

#[11]
#computing the distance of each point from the center of it's cluster. 
#With distance we can get an idea if any point is very far away from the center of the cluster we can treat it as anomaly
colnames = predictions.schema.names
distances = predictions.rdd.map(lambda x: (list(x) + [(abs(x[6]-x[5]))]))
distances = distances.toDF(colnames + ['distance'])

#[12]
distances.show()


#[13]
#What is "far" is different for each cluster. Hence finding the average distance for each cluster.
avgdist = distances.groupBy(distances.cluster).agg(avg(distances.distance).alias('avg_dist'))

#[14]
avgdist.show()

#[15]
# We take the points that lie father than 1.5 (Currently decided randomly but will have to select on the basis of previous observation) from the average to be anomaly 
distances.registerTempTable('distances')
avgdist.registerTempTable('clusters')
anomalies = spark.sql("select model, drive_days, annual_failure_rate as anomolous_failure_rate from distances inner join clusters on distances.cluster = clusters.cluster where distances.annual_failure_rate > (distances.center + clusters.avg_dist)")

#[16]
anomalies.show(5)