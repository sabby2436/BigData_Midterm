#This is coded according to zepplin, language: Scala
#I was able to run this on zeppelin but before I save my browser crashed

#References
#https://www.duedil.com/engineering/efficient-broadcast-joins-using-bloom-filters
#https://spark.apache.org/docs/2.4.0/api/java/index.html?org/apache/spark/util/sketch/BloomFilter.html
#https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#broadcast-variables 



#[1]
import org.apache.spark.sql.SparkSession

#[2]
spark = SparkSession\
        .builder\
        .appName("Q1")\
        .config("1.", "1")\   
        .getOrCreate()

spark



#[3]
val tableA = spark.read.format("csv").option("header", "true").load("/FileStore/tables/2019_03_31-ec88f.csv")

#[4]
tableA.count()

#[5]
val tableB = spark.read.format("csv").option("header", "true").load("/FileStore/tables/table%20B/2019_03_31-ec88f.csv")

#[6]
tableB.count()

#[7]
val bloomfil= small.stat.bloomFilter("model",small.count(),0.1) 

#[8]
val bcloomfil = spark.sparkContext.broadcast(bloomfil)

#[9]
val convalue= udf((x:String)=> if(x!=null) bcVarbloomfil.value.mightContainString(x) else false)

#[10]
import org.apache.spark.sql.function.col 

#[11]
fbig.where(convalue(col("model"))).toDF()

#[12]
fbig.count()