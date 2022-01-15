from pyspark import SparkContext
from pyspark.streaming import StreamingContext,StreamingListener
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract
from pyspark.sql import functions as func

#Create a sparkSession
spark = SparkSession.builder.appName('StructuredStreaming').getOrCreate()

#Monitor the logs directory for the new log data and read in the raw lines as accessLines
accessLines = spark.readStream.text('logs')

#Parse out the common log format to a dataframe
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))
#adding the actual timestamp
logsDF=logsDF.withColumn('eventTime', func.current_timestamp())
#keep a running count of every access by status code
endpointCounts = logsDF.groupBy(func.window(func.col('eventTime'),'30 seconds',"10 seconds"),
                                func.col('endpoint')).count()
endpointCountsSorted= endpointCounts.sort('count', ascending=False)

#kick off our streaming query, dumping results to the console
query = (endpointCountsSorted.writeStream.outputMode("complete").format("console").queryName('counts').start())

#Run forever until terminated
query.awaitTermination()

