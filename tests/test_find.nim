# nim c -r --gc:arc --threads:on test_find.nim

import random
from os import sleep
import ../src/suber

let bus = newSuber[int, 1]()
var ids: array[50, MonoTime]
var i: int

proc onFind(query: MonoTime, message: ptr SuberMessage[int]) {.gcsafe, raises:[].} =
  if message == nil: echo "not found: ", query else: echo "found: ", query

proc onPush(message: ptr SuberMessage[int]) =
  {.gcsafe.}:
    echo i,"=",message.timestamp
    ids[i] = message.timestamp
    i.inc
  
randomize()
bus.setPushCallback(onPush)
bus.addTopic(1.Topic)
for x in 0 .. ids.high: bus.push(1.Topic, rand(int.high))
while bus.getChannelQueueLengths[0] > 0: sleep(10)
for x in 0 .. ids.high: bus.find(ids[rand(x)], onFind)
bus.stop()