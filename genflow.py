import random
import json as json
import time
from kafka import SimpleProducer, KafkaClient

kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka, async=True)

def generateMessages(value, count):
  queue = []
  for c in range(0, count):
    queue.append({"port":value})
  return queue

def msgByPortDist():
  pick = random.randint(0, 9)
  if pick != 0:
    portList = {80:10000,443:450,23:60,3389:20,445:15,1433:12,6000:9,22:6,8080:3,25:2}
    portIdx = random.randint(0,len(portList) - 1)
    port = portList.keys()[portIdx]
    messages = generateMessages(port, portList[port])
  else:
    messages = generateMessages(random.randint(1, 65535), 1)
  return messages

def getMessages():
  queue = []
  while len(queue) < 10000:
    tmpQueue = msgByPortDist()
    for i in tmpQueue:
        queue.append(i)
  random.shuffle(queue)
  print "queue size: {q}".format(q=len(queue))
  print "queue filled and shuffled, sending to kafka"
  return queue 

while True:
  print "getting messages"
  for m in getMessages(): 
    #with open("generatedData.json", 'a') as outFile:
  #    outFile.write(json.dumps(m) + "\r\n")
    producer.send_messages("dendro",json.dumps(m))
  print "finished sending to kafka"
  time.sleep(10)
  #outFile.close()
