# nim c -r --gc:orc --d:release test_delivery.nim

from os import sleep
import locks, ../src/suber

const DeliveryCount = 100000
const ThreadCount = 4

var pushedmessages, aDeliveredmessages, bDeliveredmessages: IntSet

proc len(i: int): int = 1

proc onADeliver(messages: openArray[ptr SuberMessage[int, int]]) =
  {.gcsafe.}:
    for m in messages: aDeliveredmessages.incl(m.data)

proc onBDeliver(messages: openArray[ptr SuberMessage[int, int]]) =
  {.gcsafe.}:
    for m in messages: bDeliveredmessages.incl(m.data)

var a = newSuber[int, int](onADeliver)

var b = newSuber[int, int](onBDeliver, 100, 20000, 10000, 50)

var lock: Lock

proc run(t: int) =
  {.gcsafe.}:
    var i = 0
    while i != DeliveryCount:
      i.inc
      if i mod 1000 == 0: a.doDelivery()
      let data = i+(t*DeliveryCount)
      a.push(1, data)
      b.push(1, data)
      withLock(lock): pushedmessages.incl(data)

var threads: array[ThreadCount, Thread[int]]
discard a.subscribe(1, 1, true)
discard b.subscribe(1, 1, true)
lock.initLock
echo "Multi-threaded delivery testing with ", ThreadCount * DeliveryCount, " messages"
for i in 0 ..< ThreadCount: createThread(threads[i], run, i)
joinThreads(threads)
a.doDelivery()
b.doDelivery()
sleep(1000)
doAssert(pushedmessages.len == ThreadCount * DeliveryCount)
doAssert(pushedmessages.len == aDeliveredmessages.len)
doAssert(aDeliveredmessages.len == bDeliveredmessages.len)
echo "ok"