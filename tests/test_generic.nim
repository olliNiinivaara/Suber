# nim c -r --gc:orc -d:release test_generic.nim

import os, random, times, std/monotimes
import ../src/suber

const TestDuration = initDuration(seconds = 10)
const MaxTopics = 10000

type
  MessageData = object
    first: string
    second: string
    messagenumber: int

  Message = SuberMessage[MessageData]

  TopicData  = object
    topic: Topic
    subscribers: IntSet

var
  publishedmessages: int
  topics: seq[TopicData]
  subscribers: seq[Subscriber]
  aPushedmessages, bPushedmessages: int
  aDeliveredmessages, bDeliveredmessages: int

var
  addtopics, removetopics, addsubscribers, removesubscribers: int

proc onAPush(message: ptr Message) = aPushedmessages.inc

proc onBPush(message: ptr Message) = bPushedmessages.inc

proc onADeliver(messages: openArray[ptr Message]) = aDeliveredmessages += messages.len

proc onBDeliver(messages: openArray[ptr Message]) {.gcsafe, raises:[].} = bDeliveredmessages += messages.len

let a: Suber[MessageData, MaxTopics] = newSuber[MessageData, MaxTopics](onAPush, onADeliver)

let b: Suber[MessageData, MaxTopics] = newSuber[MessageData, MaxTopics](onBPush, onBDeliver, 1000, 20, 5, 10)

var rounds = 0

proc addTopic() =
  if a.getTopiccount() == MaxTopics: return
  addtopics.inc
  let newtopic = rounds.Topic
  topics.add(TopicData(topic: newtopic, subscribers: initIntSet()))
  a.addTopic(newtopic)
  b.addTopic(newtopic)
  doAssert(a.hasTopic(newtopic))
  doAssert(b.hasTopic(newtopic))

proc removeTopic() =
  if topics.len == 0: return
  removetopics.inc
  let r = rand(topics.len - 1)
  doAssert(a.hasTopic(topics[r].topic))
  doAssert(b.hasTopic(topics[r].topic))
  a.removeTopic(topics[r].topic)
  b.removeTopic(topics[r].topic)
  doAssert(not a.hasTopic(topics[r].topic))
  doAssert(not b.hasTopic(topics[r].topic))
  topics.del(r)
  var nomores: seq[Subscriber]
  for s in 0 ..< subscribers.len:
    if a.getSubscriptions(subscribers[s]).len == 0: nomores.add(subscribers[s])
  for n in nomores: subscribers.del(subscribers.find(n))

proc addSubscriber() =
  addsubscribers.inc
  if topics.len == 0: addTopic()
  let newsubscriber = rounds.Subscriber
  subscribers.add(newsubscriber)
  let r = rand(topics.len - 1)
  topics[r].subscribers.incl(newsubscriber.int)
  doAssert(a.subscribe(newsubscriber, topics[r].topic))
  doAssert(b.subscribe(newsubscriber, topics[r].topic))
  doAssert(a.getSubscribers(topics[r].topic) == topics[r].subscribers)
  doAssert(b.getSubscribers(topics[r].topic) == topics[r].subscribers)

proc removeSubscriber() =
  if subscribers.len == 0: return
  removesubscribers.inc
  let r = rand(subscribers.len - 1)
  doAssert(a.getSubscriptions(subscribers[r]).len > 0)
  doAssert(b.getSubscriptions(subscribers[r]).len > 0)
  a.unsubscribe(subscribers[r])
  b.unsubscribe(subscribers[r])
  doAssert(a.getSubscriptions(subscribers[r]).len == 0)
  doAssert(b.getSubscriptions(subscribers[r]).len == 0)
  for topicdata in topics.mitems:
    topicdata.subscribers.excl(subscribers[r].int)
  subscribers.del(r)
 
proc push() =
  publishedmessages.inc
  a.push(1.Topic, getMonotime(), MessageData(first: $rounds, second: $rounds, messagenumber: publishedmessages), ($rounds).len() * 2)
  b.push(1.Topic, getMonotime(), MessageData(first: $rounds, second: $rounds, messagenumber: publishedmessages), ($rounds).len() * 2)
 
proc run() =
  discard a.subscribe(1.Subscriber, 1.Topic, true)
  discard b.subscribe(1.Subscriber, 1.Topic, true)
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