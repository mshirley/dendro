from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import urllib2
import time

global options
options = {}

options["rate"] = 30
options["appName"] = "dendro"
options["zkQuorum"] = "localhost"
options["topic"] = "dendro"
options["consumerId"] = "spark-streaming-consumer"
options["metric"] = "port"
options["tsdbPrefix"] = "threatflow.ports"
options["url"] = "http://localhost:4242/api/put?detailed"
options["tag"] = "port"

sc = SparkContext(appName=options["appName"])
ssc = StreamingContext(sc, options["rate"])

kvs = KafkaUtils.createStream(ssc, options["zkQuorum"], options["consumerId"], {options["topic"]: 1})

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def postToTsdb(key, value, options):
    try: 
      data = {"metric": options["metric"], "timestamp": int(time.time()), "value": value, "tags":{options["tag"]:key}}
      req = urllib2.Request(options["url"], json.dumps(data), {'Content-Type': 'application/json'})
      f = urllib2.urlopen(req)
    except:
      pass

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        sqlContext = getSqlContextInstance(rdd.context)
        lineDf = sqlContext.jsonRDD(rdd)
        metrics = lineDf.groupBy(options["metric"]).count()
        for k, v in metrics.collect():
            print k, v
            postToTsdb(k,v,options) 
    except:
        pass

lines = kvs.map(lambda x: x[1])
lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()

# notes
#sqlContext = SQLContext(sc)
#df = sqlContext.read.json("/home/mshirley/src/dendro/data/test.json")
#gender = lineDf.select(lineDf["gender"] == "male").count()
#print gender 
#total = lineDf.select(lineDf["eyeColor"] == "blue").count()

#lineDf.write.parquet('/home/mshirley/src/dendro/data/test1_parquet')

#print lineDf.first()
#lineDf.registerTempTable("lines")

#lineDf = sqlContext.sql("select * from lines")
#print lineDf.show()
#print type(lineDf)

# Register as table
#wordsDataFrame.registerTempTable("words")

# Do word count on table using SQL and print it
#wordCountsDataFrame = sqlContext.sql("select word, count(*) as total from words group by word")
#wordCountsDataFrame.show()
