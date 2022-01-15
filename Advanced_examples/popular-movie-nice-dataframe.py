from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

#build the broadcasting dictionary
def loadMovieNames():
    movieNames={}
    with codecs.open("C:/Users/Ahmed/Desktop/Formation_Machine-Learning/SparkCourse/ml-100k/u.ITEM",
                      mode= "r",encoding='ISO-8859-1', errors="ignore"
                      ) as f:
        for line in f:
            id= line.split('|')[0]
            name=line.split('|')[1]
            movieNames[int(id)]=name
    return movieNames

#Create the broadcasting object
#Broadcast a read-only variable to the cluster
#Returning a Broadcast object for reading it in distributed functions.
#The variable will be sent to each cluster only once.
nameDict= spark.sparkContext.broadcast(loadMovieNames())


# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
moviesDF = spark.read.csv("C:/Users/Ahmed/Desktop/Formation_Machine-Learning/SparkCourse/ml-100k/u.data",
                           schema=schema,
                           header=True,
                           sep='\t')

topMovieIDs=moviesDF.groupBy('movieID').count().sort('count',ascending=False)

#create a user defined function to lookup movie names from our broadcasted dictionary
def lookupName(movieID):
    return nameDict.value[movieID]
lookupNameUDF= func.udf(lookupName)

#add a movieTitle col using our new udf
moviesWithNames=topMovieIDs.withColumn('movieTitle', lookupNameUDF(func.col('movieID')))

# Grab the top 10
moviesWithNames.show(moviesWithNames.count())