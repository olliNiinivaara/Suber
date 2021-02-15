# nim c -r --gc:arc --threads:on --d:danger test_speed.nim

from os import sleep
import ../src/suber

const TestDuration = 10
const ThreadCount = 2
let timestamp = getMonoTime()

var messagecount: int

proc onDeliver(messages: openArray[ptr SuberMessage[int]]) = {.gcsafe.}: discard messagecount.atomicInc(messages.len)
  
#var bus: Suber[int, int, 1] = newSuber(onDeliver, 1000000, 100000, 100)
let bus = newSuber[int, 1](onDeliver, 1000000, 100000, 100)
#bus.initSuber(nil, 1000000, 100000, 100)
discard bus.subscribe(1.Subscriber, 1.Topic, true)

var stop: bool

proc run() =
  {.gcsafe.}:
    while not stop:
      bus.push(1.Topic, timestamp, 1)
      
echo "Speed testing for ", TestDuration, " seconds "
var threads: array[ThreadCount, Thread[void]]
for i in 0 ..< ThreadCount: createThread(threads[i], run)
sleep TestDuration * 1000
stop = true
joinThreads(threads)
sleep(1000)
echo messagecount div TestDuration, " msg/s"