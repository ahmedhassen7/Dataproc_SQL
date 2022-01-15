from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

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

# Some SQL-style magic to sort all movies by popularity in one line!
#topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

topMovieIDs=moviesDF.groupBy('movieID').count().sort('count',ascending=False)

# Grab the top 10
topMovieIDs.show(10)

