# nim c -r --gc:arc --threads:on --d:danger test_speed.nim

from os import sleep
import ../src/suber

const TestDuration = 10
const ThreadCount = 3

var messagecount: int

proc onDeliver(messages: openArray[ptr SuberMessage[int, int]]) = {.gcsafe.}: discard messagecount.atomicInc(messages.len)
  
var bus: Suber[int, int, 1]
bus.initSuber(onDeliver, 1000000, 100000, 100, 100)
discard bus.subscribe(1, 1, true)

var stop: bool

proc run() =
  {.gcsafe.}:
    while not stop: bus.push(1, data=1)
      
echo "Speed testing for ", TestDuration, " seconds "
var threads: array[ThreadCount, Thread[void]]
for i in 0 ..< ThreadCount: createThread(threads[i], run)
sleep TestDuration * 1000
stop = true
joinThreads(threads)
sleep(1000)
echo messagecount div TestDuration, " msg/s"