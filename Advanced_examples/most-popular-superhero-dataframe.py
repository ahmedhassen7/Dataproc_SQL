from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

spark = SparkSession.builder.appName("PopularSuperHero").getOrCreate()

def mapper(input):
    hero_id=input.split(' ')[0]
    connection=len(input.split(' '))
    return Row(ID=int(hero_id),
                connection=connection-1)
def loadHeroNames():
    SuperHeroNames={}
    with codecs.open("C:/Users/Ahmed/Desktop/Formation_Machine-Learning/SparkCourse/ml-100k/Marvel-Names.txt",
                      mode= "r",encoding='ISO-8859-1', errors="ignore"
                      ) as f:
        for line in f:
            id= line.split(' ')[0]
            name=line.split(' ')[1:]
            SuperHeroNames[int(id)]=' '.join(x for x in name)
    return SuperHeroNames

lines = spark.sparkContext.textFile("C:/Users/Ahmed/Desktop/Formation_Machine-Learning/SparkCourse/ml-100k/Marvel-Graph.txt")
heroes = lines.map(mapper)
heroes_dict=spark.sparkContext.broadcast(loadHeroNames())

# Infer the schema, and register the DataFrame as a table.
heroes_df = spark.createDataFrame(heroes)

groupedHeroes_df=heroes_df.groupBy('ID').agg(func.sum('connection').alias('total_connections'))
sortedHeroes_df=groupedHeroes_df.sort('total_connections', ascending=False)

#build a user defined function
def heroName(id):
    return heroes_dict.value[id]
heroName_udf=func.udf(heroName)

SuperHero_df=sortedHeroes_df.withColumn('SuperHero_Name', heroName_udf(func.col('ID')))


SuperHero_df.show()