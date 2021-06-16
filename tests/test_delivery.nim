# nim c -r --gc:arc --threads:on --d:release test_delivery.nim

from os import sleep
import locks, ../src/suber

const DeliveryCount = 10000
const ThreadCount = 4
const TopicCount = 1

var
  pushedmessages = initIntSet()
  aDeliveredmessages = initIntSet()
  bDeliveredmessages = initIntSet()

proc onADeliver(messages: openArray[ptr SuberMessage[int]]) =
  {.gcsafe.}:
    for m in messages: aDeliveredmessages.incl(m.data)

proc onBDeliver(messages: openArray[ptr SuberMessage[int]]) =
  {.gcsafe.}:
    for m in messages: bDeliveredmessages.incl(m.data)

let a = newSuber[int, TopicCount](onADeliver)
let b = newSuber[int, TopicCount](onBDeliver, 10000, 20000, 10000, 50)

var lock: Lock

proc run(t: int) =
  {.gcsafe.}:
    var i = 0
    while i != DeliveryCount:
      i.inc
      if i mod 1000 == 0: a.doDelivery()
      var data = i+(t*DeliveryCount)
      a.push(1.Topic, data)
      b.push(1.Topic, data, 1)
      withLock(lock): pushedmessages.incl(data)

var threads: array[ThreadCount, Thread[int]]
a.subscribe(1.Subscriber, 1.Topic, true)
b.subscribe(1.Subscriber, 1.Topic, true)
lock.initLock
echo "Multi-threaded delivery testing with ", ThreadCount * DeliveryCount, " messages"
for i in 0 ..< ThreadCount: createThread(threads[i], run, i)
joinThreads(threads)
a.doDelivery()
b.doDelivery()
joinThreads([a.stop(), b.stop()])
doAssert(pushedmessages.len == ThreadCount * DeliveryCount)
doAssert(pushedmessages.len == aDeliveredmessages.len)
doAssert(aDeliveredmessages.len == bDeliveredmessages.len)
echo "ok"
echo "a Max channel queue length: ", a.getChannelQueueLengths[2]
echo "b Max channel queue length: ", b.getChannelQueueLengths[2]
sleep(1000)

#--------------------------
# gc:orc may SIGSEGV here, it is not a Suber bug!
pushedmessages.clear()
aDeliveredmessages.clear()
bDeliveredmessages.clear()
#--------------------------