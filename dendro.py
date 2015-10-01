from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import ujson as json
import urllib2
import time

sc = SparkContext(appName="dendro")
ssc = StreamingContext(sc, 30)
zkQuorum = "localhost"
topic = "dendro"
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def postToTsdb(key, value):
    data = {"metric": "flow-ports", "timestamp": int(time.time()), "value": value, "tags":{"port":key}}
    url = 'http://localhost:4242/api/put?detailed'
    req = urllib2.Request(url, json.dumps(data), {'Content-Type': 'application/json'})
    f = urllib2.urlopen(req)

def process(time, rdd):
    total = 0
    print("========= %s =========" % str(time))
    try:
        sqlContext = getSqlContextInstance(rdd.context)
        lineDf = sqlContext.jsonRDD(rdd)
	ports = lineDf.groupBy("port").count()
        for k, v in ports.collect():
            print k, v
	    postToTsdb(k,v) 
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
