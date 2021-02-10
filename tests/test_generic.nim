# nim c -r --gc:orc -d:release test_generic.nim

import os, random, times, std/monotimes
import ../suber

const TestDuration = initDuration(seconds = 10)

type
  MessageData = object
    first: string
    second: string

  Message = SuberMessage[string, MessageData]

  TopicData  = object
    name: string
    currenttopicstate: int
    subscribers: IntSet

var
  publishedmessages: int
  topics: seq[TopicData]
  subscribers: seq[int]
  aPushedmessages, bPushedmessages: int
  aDeliveredmessages, bDeliveredmessages: int

var
  addtopics, removetopics, addsubscribers, removesubscribers: int

proc len(m: MessageData): int = m.first.len + m.second.len

proc onAPush(message: ptr Message, subscribers: IntSet) = aPushedmessages.inc

proc onBPush(message: ptr Message, subscribers: IntSet) = bPushedmessages.inc

proc onADeliver(messages: openArray[ptr Message]) {.gcsafe, raises:[].} =
  aDeliveredmessages += messages.len

proc onBDeliver(messages: openArray[ptr Message]) {.gcsafe, raises:[].} =
  bDeliveredmessages += messages.len

var a = initSuber[string, MessageData](onAPush, onADeliver)

var b = initSuber[string, MessageData](onBPush, onBDeliver, 1000, 20, 5, 10)

var rounds = 0

proc addTopic() =
  if a.getTopiccount() == SuberMaxTopics: return
  addtopics.inc
  let newtopic = "topic" & $rounds
  topics.add(TopicData(name: newtopic, subscribers: initIntSet()))
  a.addTopic(newtopic)
  b.addTopic(newtopic)
  doAssert(a.hasTopic(newtopic))
  doAssert(b.hasTopic(newtopic))

proc removeTopic() =
  if topics.len == 0: return
  removetopics.inc
  let r = rand(topics.len - 1)
  doAssert(a.hasTopic(topics[r].name))
  doAssert(b.hasTopic(topics[r].name))
  a.removeTopic(topics[r].name)
  b.removeTopic(topics[r].name)
  doAssert(not a.hasTopic(topics[r].name))
  doAssert(not b.hasTopic(topics[r].name))
  topics.del(r)
  var nomores: seq[int]
  for s in 0 ..< subscribers.len:
    if not a.hasSubscriber(subscribers[s]): nomores.add(subscribers[s])
  for n in nomores: subscribers.del(subscribers.find(n))

proc addSubscriber() =
  addsubscribers.inc
  if topics.len == 0: addTopic()
  let newsubscriber = rounds
  subscribers.add(newsubscriber)
  let r = rand(topics.len - 1)
  topics[r].subscribers.incl(newsubscriber)
  doAssert(a.subscribe(newsubscriber, topics[r].name) > -1)
  doAssert(b.subscribe(newsubscriber, topics[r].name) > -1)
  doAssert(a.getSubscribers(topics[r].name) == topics[r].subscribers)
  doAssert(b.getSubscribers(topics[r].name) == topics[r].subscribers)

proc removeSubscriber() =
  if subscribers.len == 0: return
  removesubscribers.inc
  let r = rand(subscribers.len - 1)
  doAssert(a.hasSubscriber(subscribers[r]))
  doAssert(b.hasSubscriber(subscribers[r]))
  a.unsubscribe(subscribers[r])
  b.unsubscribe(subscribers[r])
  doAssert(not a.hasSubscriber(subscribers[r]))
  doAssert(not b.hasSubscriber(subscribers[r]))  
  for topicdata in topics.mitems:
    topicdata.subscribers.excl(subscribers[r])
  subscribers.del(r)
 
proc push() =
  a.push(MessageData(first: $rounds, second: $rounds), "topic")
  b.push(MessageData(first: $rounds, second: $rounds), "topic")
  publishedmessages.inc
 
proc run() =
  discard a.subscribe(-1, "topic", true)
  discard b.subscribe(-1, "topic", true)
  randomize()
  let startTime = getMonoTime()
  echo "Single-threaded generic testing for ", TestDuration
  while getMonoTime() - startTime < TestDuration:
    rounds.inc
    if rounds mod 1000 == 0: a.doDelivery()
    let action = rand(4)
    case action:
      of 0: addTopic()
      of 1: removeTopic()
      of 2: addSubscriber()
      of 3: removeSubscriber()
      else: push()
  a.doDelivery()
  b.doDelivery()

run()
sleep(1000)
doAssert(publishedmessages > 1000)
doAssert(publishedmessages == aPushedmessages)
doAssert(aPushedmessages == bPushedmessages)
doAssert(publishedmessages == aDeliveredmessages)
doAssert(aDeliveredmessages == bDeliveredmessages)
echo "ok"