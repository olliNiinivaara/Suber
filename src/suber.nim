# (C) Olli Niinivaara, 2021
# MIT Licensed

## A Pub/Sub engine.
## 
## Receives messages from multiple sources and forwards them as ordered serialized stream.
## Keeps a message cache so that subscribers can sync their state after a communication breakdown.
##
## 
## A Complete Example
## ==================
##
## .. code-block:: Nim
## 
##

static: doAssert(compileOption("threads"), "Suber requires threads:on compiler option")

import cpuinfo, intsets, tables, sets, stashtable
export intsets, tables

const SuberMaxTopics* {.intdefine.} = 10000

type
  SuberError* = object of CatchableError

  TData* = concept s
    s.len is Ordinal

  PullCallback*[TTopic, TData] = proc(topicswithexpiredstates: openArray[TTopic],
   messages: openArray[ptr SuberMessage[TTopic, TData]]) {.gcsafe, raises:[].}
  
  SuberMessageKind* = enum
    smNil,
    smMessage,
    smDeliver,
    smPull,
    smGet

  SuberMessage*[TTopic, TData] = object
    case kind*: SuberMessageKind
    of smMessage:
      topics*: Table[TTopic, int] # topicname, topicstate
      data*: TData
    of smPull:
      subscriber: int
      requests: Table[TTopic, int]
      pullcallback: PullCallback[TTopic, TData]
    of smGet:
      gettopicname: TTopic
      gettopicstate: int
      getcallback: proc(message: ptr SuberMessage[TTopic, TData]) {.gcsafe, raises:[].}
    else: discard

  DeliverCallback*[TTopic, TData] = proc(messages: openArray[ptr SuberMessage[TTopic, TData]]) {.gcsafe, raises:[].}
  
  PushCallback*[TTopic, TData] = proc(message: ptr SuberMessage[TTopic, TData], subscribers: IntSet) {.gcsafe, raises:[].}

  TopicData  = object
    currenttopicstate: int
    earliesttopicstate: int
    subscribers: IntSet

  Suber*[TTopic, TData] = object
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
    thread: Thread[ptr Suber[TTopic, TData]]


{.push checks:off.}

proc `=copy`[TTopic, TData](dest: var SuberMessage[TTopic, TData]; source: SuberMessage[TTopic, TData]) {.error.}

proc initTopicData(): TopicData =
  result.earliesttopicstate = int.high
  result.currenttopicstate = 0
  result.subscribers = initIntSet()
  
# topics ----------------------------------------------------

proc addTopic*[TTopic, TData](suber: Suber[TTopic, TData], topic: TTopic) =
  if topic == "": return
  if suber.topicdatatable.insert(topic, initTopicData())[0] == NotInStash: raise newException(SuberError, "SuberMaxTopics already in use")
  
proc removeTopic*[TTopic, TData](suber: Suber[TTopic, TData], topic: TTopic) =
  suber.topicdatatable.del(topic)

proc hasTopic*[TTopic, TData](suber: Suber[TTopic, TData], topic: TTopic): bool =
  not (findIndex(suber.topicdatatable, topic) == NotInStash)

proc getTopiccount*[TTopic, TData](suber: Suber[TTopic, TData]): int = suber.topicdatatable.len

proc getTopicInfo*[TTopic, TData](suber: Suber[TTopic, TData]): seq[tuple[name: TTopic, state: int, subscribers: IntSet]] =
  for (topicname , index) in suber.topicdatatable.keys():
    suber.topicdatatable.withFound(topicname, index):
      result.add((topicname, value.currenttopicstate, value.subscribers))

# subscribe ----------------------------------------------------

proc subscribe*[TTopic, TData](suber: Suber[TTopic, TData], subscriber: int, topic: TTopic, createnewtopic = false): int =
  withValue(suber.topicdatatable, topic):
    value.subscribers.incl(subscriber)
    return value.currenttopicstate
  do:  
    if not createnewtopic: return -1
    var newtopicdata = initTopicData()
    newtopicdata.subscribers.incl(subscriber)
    let insertresult = suber.topicdatatable.insert(topic, newtopicdata)
    return if insertresult[1]: 0 else: -2

proc unsubscribe*[TTopic, TData](suber: Suber[TTopic, TData], subscriber: int, topic: TTopic = "") =
  if topic == "":
    for (topicname , index) in suber.topicdatatable.keys():
      suber.topicdatatable.withFound(topicname, index): value.subscribers.excl(subscriber)
  else:
    suber.topicdatatable.withValue(topic): value.subscribers.excl(subscriber)
        
proc getSubscriptions*[TTopic, TData](suber: Suber[TTopic, TData], subscriber: int): seq[TTopic] =
  for (topicname , index) in suber.topicdatatable.keys():
    suber.topicdatatable.withFound(topicname, index):
      if value.subscribers.contains(subscriber): result.add(topicname)

proc getSubscribers*[TTopic, TData](suber: Suber[TTopic, TData], topicname: TTopic): IntSet =
  suber.topicdatatable.withValue(topicname): return value.subscribers

proc getSubscribers*[TTopic, TData](suber: Suber[TTopic, TData]): IntSet =
  for (topicname , index) in suber.topicdatatable.keys():
    suber.topicdatatable.withFound(topicname, index): result.incl(value.subscribers)
      
proc getSubscribers*[TTopic, TData](suber: Suber[TTopic, TData], topics: openArray[TTopic]): IntSet =
  for topicname in topics:
    suber.topicdatatable.withValue(topicname): result.incl(value.subscribers)

proc getSubscribers*[TTopic, TData](suber: Suber, message: ptr SuberMessage[TTopic, TData], toset: var IntSet) =
  for topicname in message.topictable.keys():
    suber.topicdatatable.withValue(topicname): toset.incl(value.subscribers)

proc isSubscriber*[TTopic, TData](suber: Suber[TTopic, TData], subscriber: int, topicname: TTopic): bool =
  suber.topicdatatable.withValue(topicname): return value.subscribers.contains(subscriber)

proc hasSubscriber*(suber: Suber, subscriber: int): bool =
  for (topicname , index) in suber.topicdatatable.keys():
    suber.topicdatatable.withFound(topicname, index):
      if value.subscribers.contains(subscriber): return true
  false

# deliver ------------------------------------------------

proc doDelivery*[TTopic, TData](suber: var Suber[TTopic, TData]) =
  if unlikely(suber.deliverCallback == nil): raise newException(SuberError, "deliverCallback not set")
  suber.channel.send(SuberMessage[TTopic, TData](kind: smDeliver))

template handleDelivery() =
  var messages: seq[ptr SuberMessage[TTTopic, TTData]]
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

proc push*[TTopic, TData](suber: var Suber[TTopic, TData], data: sink TData, topics: sink Table[TTopic, int]) =
  if unlikely(data.len > suber.CacheMaxCapacity div 2):
    raise newException(SuberError, "data.len (" & $data.len & ") > suber.CacheMaxCapacity div 2 (" & $(suber.CacheMaxCapacity div 2) & ")")
  suber.channel.send(SuberMessage[TTopic, TData](kind: smMessage, topics: move topics, data: move data))

proc push*[TTopic, TData](suber: var Suber[TTopic, TData], data: sink TData, topics: varargs[TTopic]) =
  if unlikely(topics.len == 0): return
  var table: Table[TTopic, int]
  for topic in topics: table[topic] = -1
  suber.channel.send(SuberMessage[TTopic, TData](kind: smMessage, topics: move table, data: move data))

template incEarliests(index: int) =
  for topicname in suber.cache[index].topics.keys():
    suber.topicdatatable.withValue(topicname):
      if value.earliesttopicstate == suber.cache[index].topics[topicname]:
        value.earliesttopicstate.inc
        if (unlikely) value.earliesttopicstate > value.currenttopicstate: value.earliesttopicstate = int.high

template addToCache() =
  if (unlikely) suber.cachesize + message.data.len() > suber.CacheMaxCapacity:
    when not defined(release):
      echo "suber cache size ", suber.cachesize, " + message ", + message.data.len(), " > CacheMaxCapacity ", suber.CacheMaxCapacity
    if suber.deliverCallback != nil: handleDelivery()
    var current = suber.head + 1
    if (unlikely) current >= suber.cache.len: current = 0
    var targetsize = suber.CacheMaxCapacity div 2
    while suber.cachesize + message.data.len() > targetsize:
      if suber.cache[current].kind == smMessage:
        suber.cachesize -= suber.cache[current].data.len()
        incEarliests(current)
        suber.cache[current] = SuberMessage[TTTopic, TTData](kind: smNil)
      current.inc
      if current == suber.cache.len: current = 0
      if (unlikely) current == suber.head: break
    when not defined(release): echo "size after invalidation: ", suber.cachesize + message.data.len()

  if suber.deliverCallback != nil:
    if ((suber.head == suber.CacheMaxLength - 1 and suber.lastdelivered < 1) or suber.lastdelivered == suber.head + 1): handleDelivery()
    elif suber.DeliveryMaxSize > 0:
      let deliverysize =
        if suber.lastdelivered < suber.head: suber.head - suber.lastdelivered
        else: suber.CacheMaxLength - (suber.head - suber.lastdelivered)
      if deliverysize >= suber.DeliveryMaxSize: handleDelivery()

  suber.head.inc
  suber.cachesize += message.data.len()

  if suber.cache.len < suber.CacheMaxLength: suber.cache.add(message)
  else:
    if (unlikely) suber.head == suber.CacheMaxLength: suber.head = 0
    if suber.cache[suber.head].kind == smMessage:
      suber.cachesize -= suber.cache[suber.head].data.len
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

proc getMessage*[TTopic, TData](suber: var Suber[TTopic, TData], topic: TTopic, topicstate: int,
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

proc getCurrenttopicstate*[TTopic, TData](suber: Suber[TTopic, TData], topic: TTopic): int =
  suber.topicdatatable.withValue(topic): return value.currenttopicstate
  do: return -1
  
# pull ----------------------------------------------------

proc pull*[TTopic, TData](suber: var Suber[TTopic, TData], subscriber: int, pullrequests: sink Table[TTopic, int],
 callback: PullCallback[TTopic, TData]) =
  if unlikely(pullrequests.len() == 0 or suber.head == -1): return
  for topicname in pullrequests.keys:
    suber.topicdatatable.withValue(topicname):
      if not value.subscribers.contains(subscriber): return
    do:
      return
  suber.channel.send(SuberMessage[TTopic, TData](kind: smPull, subscriber: subscriber, requests: move pullrequests, pullcallback: callback))

proc pull*[TTopic, TData](suber: var Suber[TTopic, TData], subscriber: int,
 topics: openArray[tuple[topicname: TTopic, aftertopicstate: int]], callback: PullCallback[TTopic, TData]) =
  if (likely) topics.len > 0:
    var table: Table[TTopic, int]
    for topic in topics: table[topic.topicname] = topic.aftertopicstate
    pull(suber, subscriber, table, callback)

proc pull*[TTopic, TData](suber: var Suber[TTopic, TData], subscriber: int, topicname: sink TTopic, aftertopicstate: int,
 callback: PullCallback[TTopic, TData]) {.inline.} =
  pull(suber, subscriber, [(move topicname, aftertopicstate)], callback)

template handlePull() =
  var topicswithexpiredstates: seq[TTTopic]
  var messages: seq[ptr SuberMessage[TTTopic, TTData]]
 
  for topicname, aftertopicstate in message.requests.pairs:
    suber.topicdatatable.withValue(topicname):
      if unlikely(aftertopicstate > value.currenttopicstate or value.earliesttopicstate > aftertopicstate + 1): topicswithexpiredstates.add(topicname)
    do:
      topicswithexpiredstates.add(topicname)
  
  for topicname in topicswithexpiredstates: message.requests.del(topicname)
  
  if message.requests.len > 0:
    var remainingtopics: HashSet[TTTopic]
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
  message.pullcallback(topicswithexpiredstates, messages)

# run ----------------------------------------------------

proc run[TTTopic, TTData: TData](suber: ptr Suber[TTTopic, TTData]) {.thread, nimcall.} =
  while true:
    var message = suber.channel.recv()
    case message.kind
      of smMessage: handlePush()
      of smDeliver: handleDelivery()
      of smPull: handlePull()
      of smGet: handleGet()
      of smNil: (suber.channel.close(); break)

proc initSuber*[TTTopic, TTData: TData](
 onPush: PushCallback[TTTopic, TTData], onDeliver: DeliverCallback[TTTopic, TTData],
 cachemaxcapacity = 10000000, cachelength = 1000000, maxdeliverysize = -1, channelsize = 200): Suber[TTTopic, TTData] =
  assert(cachelength > channelsize)
  assert(maxdeliverysize < cachelength)
  doAssert((countProcessors() == 0) or (channelsize >= countProcessors()))
  result.CacheMaxCapacity = cachemaxcapacity
  result.CacheMaxLength = cachelength
  result.DeliveryMaxSize = maxdeliverysize
  result.topicdatatable = newStashTable[TTTopic, TopicData, SuberMaxTopics]()
  result.channel.open(channelsize)
  result.pushCallback = onPush
  result.deliverCallback = onDeliver    
  result.cache = newSeqOfCap[SuberMessage[TTTopic, TTData]](result.CacheMaxLength)
  result.head = -1
  result.lastdelivered = -1
  createThread(result.thread, run, addr result)

proc stopSuber*(suber: var Suber) =
  suber.channel.send(SuberMessage(kind: smNil))

{.pop.}