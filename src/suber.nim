# (C) Olli Niinivaara, 2021
# MIT Licensed

## A Pub/Sub engine.
## 
## Receives messages from multiple sources and delivers them as serialized stream.
## Sources can be in different threads.
## Reading does not block writing and writes do not block reading or each other.
## Messages can belong to multiple topics.
## Subscribers can subscribe to multiple topics.
## Topics, subscribers and subscriptions can be modified anytime.
## Delivery may be triggered on message push, size of undelivered messages, amount of undelivered messages and on call.
## Keeps a message cache so that subscribers can sync their state at will.
##
## 
## Example
## ==================
##
## .. code-block:: Nim
##  
##  # nim c -r --gc:arc --threads:on --d:release example.nim
##  import suber, os, random
##  
##  let topics = ["Art", "Science", "Nim", "Fishing"]
##  let subscribers = ["Amy", "Bob", "Chas", "Dave"]
##  let messagedatas = ["Good News", "Bad News", "Breaking News", "Old News"]
##  let publishers = ["Helsinki", "Tallinn", "Oslo", "Edinburgh"]
##  var bus: Suber[string, string, 4] # string topics, string datas, max 4 topics
##  
##  proc writeMessages(subscriber: int, messages: openArray[ptr SuberMessage[string, string]]) =
##    var subscriberids: IntSet
##    try:
##      for message in messages:
##        if subscriber > -1: stdout.write("Redelivery to " & subscribers[subscriber] & ", ")
##        else:
##          stdout.write("Delivery to ")      
##          subscriberids.clear() 
##          bus.getSubscribers(message, subscriberids)
##          for subscriberid in subscriberids: stdout.write(subscribers[subscriberid] & ", ")
##        stdout.write("concerning ")
##        for (topic , state) in message.topics.pairs(): stdout.write(topic & " (" & $state & ") & ")
##        stdout.writeLine("\b\b: " & message.data) ; stdout.flushFile()
##    except: discard # write IOError
##  
##  proc onDeliver(messages: openArray[ptr SuberMessage[string, string]]) {.gcsafe, raises:[].} =
##    {.gcsafe.}: writeMessages(-1, messages)
##    
##  proc onPullrequest(subscriber: int, topicswithexpiredstates: openArray[string],
##   messages: openArray[ptr SuberMessage[string, string]]) {.gcsafe, raises:[].} =
##    if topicswithexpiredstates.len > 0:
##      echo "Sorry, we are not stocking that old news. Maybe ask for a reprint?"
##    else:
##      {.gcsafe.}: writeMessages(subscriber, messages)
##  
##  var stop: bool
##  
##  proc generateMessages(publisher: int) =
##    {.gcsafe.}:
##      while not stop:
##        write(stdout, $publisher); flushFile(stdout)
##        var messagetopics: seq[string]
##        for topicnumber in 1 .. 1 + rand(2): messagetopics.add(topics[rand(3)])
##        bus.push(messagetopics, messagedatas[rand(3)] & " from " & publishers[publisher])
##        sleep(100+rand(500))
##  
##  proc deliver() =
##    {.gcsafe.}:
##      for i in 1 .. 10:
##        sleep(1000); echo ""
##        bus.doDelivery()
##      stop = true
##  
##  initSuber[string, string, 4](bus, onDeliver)
##  
##  for i in 0 ..< 4:
##    for j in 0 .. i:
##      discard bus.subscribe(i, topics[j], true)
##  
##  var delivererthread: Thread[void]
##  createThread(delivererthread, deliver)
##  var publisherthreads: array[4, Thread[int]]
##  for i in 0 ..< 4: createThread(publisherthreads[i], generateMessages, i)
##  joinThreads(publisherthreads)
##  joinThread(delivererthread)
##  
##  echo "----"
##  sleep(2000)
##  echo("Chas requests redelivery of Nim-related news since 30th Nim-related message!")
##  bus.pull(subscribers.find("Chas"), "Nim", 30, onPullrequest)
##  sleep(1000)
##
##
##
## Documentation in progress...
## ============================
## 

static: doAssert(compileOption("threads"), "Suber requires threads:on compiler option") 
  
import cpuinfo, intsets, tables, sets, stashtable
export intsets, tables

type
  SuberError* = object of CatchableError

  PullCallback*[TTopic; TData] = proc(subscriber: int, topicswithexpiredstates: openArray[TTopic],
   messages: openArray[ptr SuberMessage[TTopic, TData]]) {.gcsafe, raises:[].}
  
  SuberMessageKind* = enum
    smNil,
    smMessage,
    smDeliver,
    smPull,
    smGet

  SuberMessage*[TTopic; TData] = object
    case kind*: SuberMessageKind
    of smMessage:
      topics*: Table[TTopic, int] # topicname, topicstate
      data*: TData
      size: int
    of smPull:
      subscriber: int
      requests: Table[TTopic, int]
      pullcallback: PullCallback[TTopic, TData]
    of smGet:
      gettopicname: TTopic
      gettopicstate: int
      getcallback: proc(message: ptr SuberMessage[TTopic, TData]) {.gcsafe, raises:[].}
    else: discard

  DeliverCallback*[TTopic; TData] = proc(messages: openArray[ptr SuberMessage[TTopic, TData]]) {.gcsafe, raises:[].}
  
  PushCallback*[TTopic; TData] = proc(message: ptr SuberMessage[TTopic, TData], subscribers: IntSet) {.gcsafe, raises:[].}

  TopicData  = object
    currenttopicstate: int
    earliesttopicstate: int
    subscribers: IntSet

  Suber*[TTopic; TData; SuberMaxTopics: static int] = object
    CacheMaxCapacity: int
    CacheMaxLength: int 
    DeliveryMaxSize: int
    cache: seq[SuberMessage[TTopic, TData]]
    cachesize: int
    head: int
    lastdelivered: int
    deliverCallback: DeliverCallback[TTopic, TData]
    pushCallback: PushCallback[TTopic, TData]
    topicdatatable: StashTable[TTopic, TopicData, SuberMaxTopics]
    channel: Channel[SuberMessage[TTopic, TData]]
    thread: Thread[ptr Suber[TTopic, TData, SuberMaxTopics]]
    peakchannelqueuelength: int
    maxchannelqueuelength: int
  

{.push checks:off.}

proc `=copy`[TTopic; TData](dest: var SuberMessage[TTopic, TData]; source: SuberMessage[TTopic, TData]) {.error.}

proc initTopicData(): TopicData =
  result.earliesttopicstate = int.high
  result.currenttopicstate = 0
  result.subscribers = initIntSet()

proc run[TTopic; TData; SuberMaxTopics](suber: ptr Suber[TTopic, TData, SuberMaxTopics]) {.thread, nimcall.}

proc initSuber*[TTopic; TData; SuberMaxTopics](
 suber: var Suber[TTopic, TData, SuberMaxTopics], onPush: PushCallback[TTopic, TData], onDeliver: DeliverCallback[TTopic, TData],
 cachemaxcapacity = 10000000, cachelength = 1000000, maxdeliverysize = -1, channelsize = 200) =
  assert(cachelength > channelsize)
  assert(maxdeliverysize < cachelength)
  doAssert(channelsize == 0 or (countProcessors() == 0) or (channelsize >= countProcessors()))
  suber.CacheMaxCapacity = cachemaxcapacity
  suber.CacheMaxLength = cachelength
  suber.DeliveryMaxSize = maxdeliverysize
  suber.topicdatatable = newStashTable[TTopic, TopicData, SuberMaxTopics]()
  suber.channel.open(channelsize)
  suber.pushCallback = onPush
  suber.deliverCallback = onDeliver    
  suber.cache = newSeqOfCap[SuberMessage[TTopic, TData]](suber.CacheMaxLength)
  suber.head = -1
  suber.lastdelivered = -1
  createThread(suber.thread, run, addr suber)

proc initSuber*[TTopic; TData; SuberMaxTopics](suber: var Suber[TTopic, TData, SuberMaxTopics], onDeliver: DeliverCallback[TTopic, TData],
 cachemaxcapacity = 10000000, cachelength = 1000000, maxdeliverysize = -1, channelsize = 200) =
  initSuber(suber, nil, onDeliver, cachemaxcapacity, cachelength, maxdeliverysize, channelsize)

proc stopSuber*(suber: var Suber) =
  suber.channel.send(SuberMessage(kind: smNil))

proc getChannelQueueLengths*(suber: var Suber): (int, int, int) =
  (suber.channel.peek(), suber.peakchannelqueuelength, suber.maxchannelqueuelength)
  
# topics ----------------------------------------------------

proc addTopic*[TTopic](suber: Suber, topic: TTopic) =
  if topic == "": return
  if suber.topicdatatable.insert(topic, initTopicData())[0] == NotInStash: raise newException(SuberError, "SuberMaxTopics already in use")
  
proc removeTopic*[TTopic](suber: Suber, topic: TTopic) =
  suber.topicdatatable.del(topic)

proc hasTopic*[TTopic](suber: Suber, topic: TTopic): bool =
  not (findIndex(suber.topicdatatable, topic) == NotInStash)

proc getTopiccount*(suber: Suber): int = suber.topicdatatable.len

proc getTopicInfo*[TTopic](suber: Suber): seq[tuple[name: TTopic; state: int, subscribers: IntSet]] =
  for (topicname , index) in suber.topicdatatable.keys():
    suber.topicdatatable.withFound(topicname, index):
      result.add((topicname, value.currenttopicstate, value.subscribers))

# subscribe ----------------------------------------------------

proc subscribe*[TTopic](suber: Suber, subscriber: int, topic: TTopic; createnewtopic = false): int =
  withValue(suber.topicdatatable, topic):
    value.subscribers.incl(subscriber)
    return value.currenttopicstate
  do:  
    if not createnewtopic: return -1
    var newtopicdata = initTopicData()
    newtopicdata.subscribers.incl(subscriber)
    let insertresult = suber.topicdatatable.insert(topic, newtopicdata)
    return if insertresult[1]: 0 else: -2

proc unsubscribe*[TTopic](suber: Suber, subscriber: int, topic: TTopic = "") =
  if topic == "":
    for (topicname , index) in suber.topicdatatable.keys():
      suber.topicdatatable.withFound(topicname, index): value.subscribers.excl(subscriber)
  else:
    suber.topicdatatable.withValue(topic): value.subscribers.excl(subscriber)
        
proc getSubscriptions*[TTopic](suber: Suber, subscriber: int): seq[TTopic] =
  for (topicname , index) in suber.topicdatatable.keys():
    suber.topicdatatable.withFound(topicname, index):
      if value.subscribers.contains(subscriber): result.add(topicname)

proc getSubscribers*[TTopic](suber: Suber, topicname: TTopic): IntSet =
  suber.topicdatatable.withValue(topicname): return value.subscribers

proc getSubscribers*(suber: Suber): IntSet =
  for (topicname , index) in suber.topicdatatable.keys():
    suber.topicdatatable.withFound(topicname, index): result.incl(value.subscribers)
      
proc getSubscribers*[TTopic](suber: Suber, topics: openArray[TTopic]): IntSet =
  for topicname in topics:
    suber.topicdatatable.withValue(topicname): result.incl(value.subscribers)

proc getSubscribers*(suber: Suber, message: ptr SuberMessage, toset: var IntSet) =
  for topicname in message.topics.keys():
    suber.topicdatatable.withValue(topicname): toset.incl(value.subscribers)

proc isSubscriber*[TTopic](suber: Suber, subscriber: int, topicname: TTopic): bool =
  suber.topicdatatable.withValue(topicname): return value.subscribers.contains(subscriber)

proc hasSubscriber*(suber: Suber, subscriber: int): bool =
  for (topicname , index) in suber.topicdatatable.keys():
    suber.topicdatatable.withFound(topicname, index):
      if value.subscribers.contains(subscriber): return true
  false

# deliver ------------------------------------------------

proc doDelivery*[TTopic; TData; SuberMaxTopics](suber: var Suber[TTopic, TData, SuberMaxTopics]) =
  if unlikely(suber.deliverCallback == nil): raise newException(SuberError, "deliverCallback not set")
  suber.channel.send(SuberMessage[TTopic, TData](kind: smDeliver))

template handleDelivery() =
  var messages: seq[ptr SuberMessage[TTopic, TData]]
  if suber.lastdelivered != suber.head:
    var current = suber.lastdelivered + 1
    if(unlikely) current == suber.CacheMaxLength: current = 0
    messages.add(addr suber.cache[current])
    while true:
      if current == suber.head: break      
      if (unlikely) current == suber.CacheMaxLength - 1: current = -1
      current.inc
      messages.add(addr suber.cache[current])
    suber.deliverCallback(messages)
    suber.lastdelivered = current

# push ----------------------------------------------------

proc push*[TTopic; TData](suber: var Suber, topics: sink Table[TTopic, int], data: sink TData, size: int = 0) =
  if unlikely(len > suber.CacheMaxCapacity div 2):
    raise newException(SuberError, "data.len (" & $len & ") > suber.CacheMaxCapacity div 2 (" & $(suber.CacheMaxCapacity div 2) & ")")
  suber.channel.send(SuberMessage[TTopic, TData](kind: smMessage, topics: move topics, data: move data, size: size))

proc push*[TTopic; TData](suber: var Suber, topics: varargs[TTopic], data: sink TData, size: int = 0) =
  if unlikely(topics.len == 0): return
  var table: Table[TTopic, int]
  for topic in topics: table[topic] = -1
  suber.channel.send(SuberMessage[TTopic, TData](kind: smMessage, topics: move table, data: move data, size: size))

template incEarliests(index: int) =
  for topicname in suber.cache[index].topics.keys():
    suber.topicdatatable.withValue(topicname):
      if value.earliesttopicstate == suber.cache[index].topics[topicname]:
        value.earliesttopicstate.inc
        if (unlikely) value.earliesttopicstate > value.currenttopicstate: value.earliesttopicstate = int.high

template addToCache() =
  if (unlikely) suber.cachesize + message.size > suber.CacheMaxCapacity:
    when not defined(release):
      echo "suber cache size ", suber.cachesize, " + message ", + message.size, " > CacheMaxCapacity ", suber.CacheMaxCapacity
    if suber.deliverCallback != nil: handleDelivery()
    var current = suber.head + 1
    if (unlikely) current >= suber.cache.len: current = 0
    var targetsize = suber.CacheMaxCapacity div 2
    while suber.cachesize + message.size > targetsize:
      if suber.cache[current].kind == smMessage:
        suber.cachesize -= suber.cache[current].size
        incEarliests(current)
        suber.cache[current] = SuberMessage[TTopic, TData](kind: smNil)
      current.inc
      if current == suber.cache.len: current = 0
      if (unlikely) current == suber.head: break
    when not defined(release): echo "size after invalidation: ", suber.cachesize + message.size

  if suber.deliverCallback != nil:
    if ((suber.head == suber.CacheMaxLength - 1 and suber.lastdelivered < 1) or suber.lastdelivered == suber.head + 1): handleDelivery()
    elif suber.DeliveryMaxSize > 0:
      let deliverysize =
        if suber.lastdelivered < suber.head: suber.head - suber.lastdelivered
        else: suber.CacheMaxLength - (suber.head - suber.lastdelivered)
      if deliverysize >= suber.DeliveryMaxSize: handleDelivery()

  suber.head.inc
  suber.cachesize += message.size

  if suber.cache.len < suber.CacheMaxLength: suber.cache.add(message)
  else:
    if (unlikely) suber.head == suber.CacheMaxLength: suber.head = 0
    if suber.cache[suber.head].kind == smMessage:
      suber.cachesize -= suber.cache[suber.head].size
      incEarliests(suber.head)
    suber.cache[suber.head] = message
  
template handlePush() =
  var subscriberset: IntSet
  addToCache()
  for topicname in suber.cache[suber.head].topics.keys():
    suber.topicdatatable.withValue(topicname):
      value.currenttopicstate.inc
      suber.cache[suber.head].topics[topicname] = value.currenttopicstate
      if (unlikely) value.earliesttopicstate == int.high: value.earliesttopicstate = value.currenttopicstate                
      if suber.pushCallback != nil: subscriberset.incl(value.subscribers)
  if suber.pushCallback != nil and subscriberset.len > 0: suber.pushCallback(addr suber.cache[suber.head], subscriberset)

# get ----------------------------------------------------

proc getMessage*[TTopic; TData](suber: var Suber, topic: TTopic; topicstate: int,
 callback: proc(message: ptr SuberMessage[TTopic, TData]) {.gcsafe, raises:[].}) =
  if (unlikely) suber.topicdatatable.findIndex(topic) == NotInStash: callback(nil)
  else: suber.channel.send(SuberMessage[TTopic, TData](kind: smGet, getcallback: callback, gettopicname: topic, gettopicstate: topicstate))

template handleGet() =
  var earliest, last: int
  suber.topicdatatable.withValue(message.gettopicname):
    earliest = value.earliesttopicstate
    last = value.currenttopicstate
  do:
    earliest = -1
  if suber.head == -1 or message.gettopicstate < earliest or message.gettopicstate >= last: earliest = -1
  var current = suber.head + 1
  if earliest != -1:
    earliest = -1
    var wrapped = false
    while true:
      current.dec
      if (unlikely) wrapped and current == suber.head: break
      if (unlikely) current == -1:
        if suber.head == suber.cache.len - 1: break
        (current = suber.cache.len ; wrapped = true; continue)
      if (unlikely) suber.cache[current].kind != smMessage: break
      if not suber.cache[current].topics.contains(message.gettopicname): continue
      let currentstate = suber.cache[current].topics[message.gettopicname]
      if currentstate == message.gettopicstate: (earliest = 1; break)
      if currentstate < message.gettopicstate: break
  if earliest == 1: message.getcallback(addr suber.cache[current])
  else: message.getcallback(nil)

proc getCurrenttopicstate*[TTopic](suber: Suber, topic: TTopic): int =
  suber.topicdatatable.withValue(topic): return value.currenttopicstate
  do: return -1
  
# pull ----------------------------------------------------

proc pull*[TTopic; TData](suber: var Suber, subscriber: int, pullrequests: sink Table[TTopic, int],
 callback: PullCallback[TTopic, TData]) =
  if unlikely(pullrequests.len() == 0 or suber.head == -1): return
  for topicname in pullrequests.keys:
    suber.topicdatatable.withValue(topicname):
      if not value.subscribers.contains(subscriber): return
    do:
      return
  suber.channel.send(SuberMessage[TTopic, TData](kind: smPull, subscriber: subscriber, requests: move pullrequests, pullcallback: callback))

proc pull*[TTopic; TData](suber: var Suber, subscriber: int,
 topics: openArray[tuple[topicname: TTopic, aftertopicstate: int]], callback: PullCallback[TTopic, TData]) =
  if (likely) topics.len > 0:
    var table: Table[TTopic, int]
    for topic in topics: table[topic.topicname] = topic.aftertopicstate
    pull(suber, subscriber, table, callback)

proc pull*[TTopic; TData](suber: var Suber, subscriber: int, topicname: sink TTopic; aftertopicstate: int,
 callback: PullCallback[TTopic, TData]) {.inline.} =
  pull(suber, subscriber, [(move topicname, aftertopicstate)], callback)

template handlePull() =
  var topicswithexpiredstates: seq[TTopic]
  var messages: seq[ptr SuberMessage[TTopic, TData]]
 
  for topicname, aftertopicstate in message.requests.pairs:
    suber.topicdatatable.withValue(topicname):
      if unlikely(aftertopicstate > value.currenttopicstate or value.earliesttopicstate > aftertopicstate + 1): topicswithexpiredstates.add(topicname)
    do:
      topicswithexpiredstates.add(topicname)
  
  for topicname in topicswithexpiredstates: message.requests.del(topicname)
  
  if message.requests.len > 0:
    var remainingtopics: HashSet[TTopic]
    for topicname in message.requests.keys(): remainingtopics.incl(topicname)
    var current = suber.head + 1
    var wrapped = false
    while true:
      current.dec
      if (unlikely) wrapped and current == suber.head: break
      if (unlikely) current == -1:
        if suber.head == suber.cache.len - 1: break
        current = suber.cache.len ; wrapped = true; continue
      if (unlikely) suber.cache[current].kind != smMessage: break
      for topicname in remainingtopics.items():
        if not suber.cache[current].topics.contains(topicname): continue
        suber.topicdatatable.withValue(topicname):
          if suber.cache[current].topics[topicname] > message.requests[topicname]:
            messages.add(addr suber.cache[current])
            break
          else:
            remainingtopics.excl(topicname)
            if remainingtopics.len == 0:
              wrapped = true
              current = suber.head + 1
  message.pullcallback(message.subscriber, topicswithexpiredstates, messages)

# run ----------------------------------------------------

proc run[TTopic; TData; SuberMaxTopics](suber: ptr Suber[TTopic, TData, SuberMaxTopics]) {.thread, nimcall.} =
  while true:
    var message = suber.channel.recv()
    let channelqueuelength = suber.channel.peek()
    if(likely) channelqueuelength == 0: suber.peakchannelqueuelength = 0
    else:
      if(unlikely) channelqueuelength > suber.peakchannelqueuelength:
        suber.peakchannelqueuelength = channelqueuelength
        if(unlikely) channelqueuelength > suber.maxchannelqueuelength:
          suber.maxchannelqueuelength = channelqueuelength  
    case message.kind
      of smMessage: handlePush()
      of smDeliver: handleDelivery()
      of smPull: handlePull()
      of smGet: handleGet()
      of smNil: (suber.channel.close(); break)

{.pop.}