
#Reference
#https://mattilyra.github.io/2017/05/23/document-deduplication-with-lsh.html
#http://mccormickml.com/2015/06/12/minhash-tutorial-with-python-code/ 
#https://databricks.com/blog/2017/05/09/detecting-abuse-scale-locality-sensitive-hashing-uber-engineering.html

#[1]
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.feature import Tokenizer, RegexTokenizer, NGram
import re as regexp
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinHashLSH
from pyspark.ml.feature import CountVectorizer


#[2]
#Creating a Spark Session
spark = SparkSession\
        .builder\
        .appName("MinHashExtraCredit")\
        .getOrCreate()

spark


#[3]
#Converting into pairs
def convert(pair):
    l = regexp.sub(r'\n\s*\n','\n',pair[1],regexp.MULTILINE)
    return [[[name for name in pair[0].split('/')][-1] ,l]]

#[4]
#flat the data
flat = spark.sparkContext.wholeTextFiles("/Users/sabarnikundu/Downloads/data_q4/*.txt").flatMap(convert)

#[5]
#convert into dataframe
data = flat.toDF(['title','content'])
data.show()

#[5]
spliter = Tokenizer(inputCol="content", outputCol="words")
split = spliter.transform(data)
split.show()

#[6]
dataset = split.select("title","content", "words")
dataset.show()

#[7]
#Going from parir of words bigrams, to trigrams, till ngrams combination of words
ng = NGram(n=2, inputCol="words", outputCol="ngrams")
dataset = ng.transform(dataset)
dataset.show()

#[8]
#fitting the model to our dataset like we do in unsupervised learning
cvect = CountVectorizer(inputCol="ngrams", outputCol="features", vocabSize=100000, minDF=2)
model = cvect.fit(dataset)
dataset = model.transform(dataset)



#[9]
#LSH class for Jaccard distance.
minhash = MinHashLSH(inputCol="features", outputCol="hashValues", seed=12345).setNumHashTables(3)
model = minhash.fit(dataset)
model.transform(dataset)



#[10]
#Printing Values
print("Total no. of Files: ",dataset.count())
print("Column Data:  ",dataset.dtypes)
dataset.show()

#[11]

matrix = model.approxSimilarityJoin(dataset, dataset, 3.0).select(col("datasetA.title").alias("A"), col("datasetB.title").alias("B"),col("distCol")).sort(desc("distCol")).dropDuplicates(['distCol'])
matrix.show()

#[12]
#setting a threshold as 0.7 (Taken Randomly for the time being but it should be based on the distCol) to decide take a decidsion based on distCol
match = matrix.filter(matrix['distCol'] > 0.7) #0.7 is the threshold value
match.show()

