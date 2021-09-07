# (C) Olli Niinivaara, 2021
# MIT Licensed

## A Pub/Sub engine.
## 
## Receives messages from multiple sources and delivers them as a serialized stream.
## Publisher threads do not block each other or delivery thread, and delivery does not block publishing.
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
##  import suber, os, random, times
##  
##  let topics = ["Art", "Science", "Nim", "Fishing"]
##  let subscribers = ["Amy", "Bob", "Chas", "Dave"]
##  let messagedatas = ["Good News", "Bad News", "Breaking News", "Old News"]
##  let publishers = ["Helsinki", "Tallinn", "Oslo", "Edinburgh"]
##  let bus = newSuber[string, 4]() # string datas, max 4 topics  
##  var stop: bool
##  
##  proc generateMessages(publisher: int) =
##    {.gcsafe.}:
##      while true:
##        if stop: break else: (sleep(100+rand(500)) ; if stop: break)
##        write(stdout, $publishers[publisher] & ", "); flushFile(stdout)
##        var messagetopics = initIntSet()
##        for topicnumber in 1 .. 1 + rand(2): messagetopics.incl(rand(3))
##        bus.push(messagetopics, messagedatas[rand(3)] & " from " & publishers[publisher])
##  
##  proc deliver() =
##    {.gcsafe.}:
##      for i in 1 .. 10: (sleep(1000); echo ""; bus.doDelivery())
##      stop = true
##  
##  proc writeMessages(subscriber: int, messages: openArray[ptr SuberMessage[string]]) =
##    var subscriberids: IntSet
##    try:
##      for message in messages:
##        if subscriber > -1:
##          stdout.write("@" & $message.timestamp & " to " & subscribers[subscriber] & ", ")
##        else:
##          stdout.write("@" & $message.timestamp & " to ")      
##          bus.getSubscribers(message, subscriberids)
##          for subscriberid in subscriberids: stdout.write(subscribers[subscriberid] & ", ")
##        stdout.write("concerning ")
##        for topic in message.topics.items(): stdout.write(topics[topic.int] & " & ")
##        stdout.writeLine("\b\b: " & message.data)
##      stdout.flushFile()
##      if subscriber > -1: echo "--"
##    except: discard # write IOError
##
##   proc onPull(subscriber: Subscriber, expiredtopics: openArray[Topic], messages: openArray[ptr SuberMessage[string]]) =
##    {.gcsafe.}:
##      if expiredtopics.len > 0: echo "Sorry, not stocking that old news anymore."
##      else: writeMessages(int(subscriber), messages)
##  
##  proc redeliver() =
##    {.gcsafe.}:
##      while true:
##        if stop: break else: (sleep(2000+rand(3000)) ; if stop: break)
##        let since = getMonoTime() - initDuration(milliseconds = 500 + rand(1500))
##        echo "" ; echo "--" ; echo("Chas requests Nim-related news since timestamp @", since)
##        let subscriber = subscribers.find("Chas")
##        bus.pull(subscriber, toIntSet([topics.find("Nim")]), since)
##  
##  proc onDeliver(messages: openArray[ptr SuberMessage[string]]) {.gcsafe, raises:[].} =
##    {.gcsafe.}: writeMessages(-1, messages)
##  
##  bus.setDeliverCallback(onDeliver)
##  bus.setPullCallback(onPull)
##  for i in 0 ..< 4: (for j in 0 .. i: bus.subscribe(i.Subscriber, j.Topic, true))
##  
##  var delivererthread: Thread[void]
##  createThread(delivererthread, deliver)
##  var redelivererthread: Thread[void]
##  createThread(redelivererthread, redeliver)
##  var publisherthreads: array[4, Thread[int]]
##  for i in 0 ..< 4: createThread(publisherthreads[i], generateMessages, i)
##  joinThreads publisherthreads ; bus.stop()
##  joinThreads([delivererthread, redelivererthread]) ; echo ""
##

when not compileOption("threads"): {.fatal: "Suber requires threads:on compiler option".}
when not defined(gcDestructors): {.fatal: "Suber requires gc:arc or orc compiler option".}

import intsets, std/monotimes, tables, stashtable
export intsets, monotimes

type
  Topic* = distinct int
  Subscriber* = distinct int
    
  SuberMessageKind = enum
    smNil
    smMessage
    smDeliver
    smPull
    smFind
    smSync

when not defined(nimdoc):
  type
    SuberMessage*[TData] = object
      case kind*: SuberMessageKind
      of smMessage:
        topics*: IntSet
        timestamp*: MonoTime
        data*: TData
        size: int
      of smPull:
        subscriber: Subscriber
        pulltopics: IntSet
        aftertimestamp: MonoTime
      of smFind:
        findtimestamp: MonoTime
        findcallback: proc(query: MonoTime, message: ptr SuberMessage[TData]) {.gcsafe, raises:[].}
      of smSync:
        syncedcallback: proc() {.gcsafe, raises:[].}
      else: discard
else:
  type
    SuberMessage*[TData] = object
      ## push, pull, deliver and find -callbacks will return (pointers to) SuberMessages.
      topics*: IntSet ## The `Topic`s that this message belongs to
      timestamp*: MonoTime ## Unique timestamp that orders and identifies messages
      data*: TData ## Message payload, a generic type
      size: int ## Size of the data, used to trigger size-based deliveries

type
  DeliverCallback*[TData] = proc(messages: openArray[ptr SuberMessage[TData]]) {.gcsafe, raises:[].}
    ## Proc that gets called when there are messages to be delivered. Set this with `setDeliverCallback`.
    ## 
    ## Note that DeliverCallback must be gcsafe and not raise any exceptions.
  
  PushCallback*[TData] = proc(message: ptr SuberMessage[TData]) {.gcsafe, raises:[].}
    ## Proc that gets called **every** time when a new message is published.
    ## Set this with `setPushCallback`.
    ## 
    ## Note that PushCallback must be gcsafe and not raise any exceptions.

  PullCallback*[TData] = proc(subscriber: Subscriber, expiredtopics: openArray[Topic],
   messages: openArray[ptr SuberMessage[TData]]) {.gcsafe, raises:[].}
    ## Proc that is called when subscriber pulls old messages.
    ## | `subscriber` identifies the puller
    ## | `expiredtopics` is list of topics for which all messages are not anymore cached
    ## | `messages` includes results for topics that had all requested messages still in cache
    ##
    ## Set this with `setPullCallback`.
    ##
    ## Note that PullCallback must be gcsafe and not raise any exceptions.

  SuberState = enum Running, Stopping, InstantStop

when not defined(nimdoc):
  type Suber*[TData; SuberMaxTopics: static int] = ref object
    channelqueuesize: int
    state: SuberState
    CacheMaxCapacity: int
    CacheMaxLength: int 
    MaxDelivery: int
    cache: seq[SuberMessage[TData]]
    cachesize: int
    head: int
    lastdelivered: int
    deliverCallback: DeliverCallback[TData]
    pushCallback: PushCallback[TData]
    pullCallback: PullCallback[TData]
    subscribers: StashTable[Topic, ref IntSet, SuberMaxTopics]
    topicexpirations: Table[Topic, MonoTime]
    channel: Channel[SuberMessage[TData]]
    thread: Thread[Suber[TData, SuberMaxTopics]]
    peakchannelqueuelength: int
    maxchannelqueuelength: int
else:
  type Suber*[TData; SuberMaxTopics: static int] = ref object
    ## The Suber service. Create one with `newSuber`.
    ## 
    ## | TData is the generic parameter that defines the type of message data
    ## | SuberMaxTopics is maximum amount of distinct topics
    ## 
    ## Note: If you have distinct topics (subscriber partitions) and application logic allows,
    ## it may be profitable to have dedicated Suber for each partition.
  

{.push checks:off.}

proc `==`*(x, y: Topic): bool {.borrow.}
proc `==`*(x, y: Subscriber): bool {.borrow.}
proc `$`*(x: Topic): string {.borrow.}
proc `$`*(x: Subscriber): string {.borrow.}

converter toTopic*(x: int): Topic = Topic(x)

type FakeMonoTime = object
  ticks: int64

proc toMonoTime*(i: int64): MonoTime {.inline.} =
  let monotime = FakeMonoTime(ticks: i)
  result = cast[MonoTime](monotime)

proc toIntSet*(x: openArray[Topic]): IntSet {.inline.} =
  result = initIntSet()
  for item in items(x): result.incl(int(item))

proc toIntSet*(x: openArray[Subscriber]): IntSet {.inline.} =
  result = initIntSet()
  for item in items(x): result.incl(int(item))

proc `=copy`[TData](dest: var SuberMessage[TData]; source: SuberMessage[TData]) {.error.}

proc run[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics]) {.thread, nimcall.}

proc initSuber[TData; SuberMaxTopics](
 suber: Suber[TData, SuberMaxTopics], onPush: PushCallback[TData], onDeliver: DeliverCallback[TData],
 cachemaxcapacity = 10000000, cachelength = 1000000, maxdelivery = -1, channelsize = 200) =
  assert(cachelength > channelsize)
  assert(maxdelivery < cachelength)
  suber.CacheMaxCapacity = cachemaxcapacity
  suber.CacheMaxLength = cachelength
  suber.MaxDelivery = maxdelivery  
  suber.subscribers = newStashTable[Topic, ref IntSet, SuberMaxTopics]()
  suber.channelqueuesize = channelsize
  suber.channel.open(channelsize)
  suber.pushCallback = onPush
  suber.deliverCallback = onDeliver    
  suber.cache = newSeqOfCap[SuberMessage[TData]](suber.CacheMaxLength)
  suber.head = -1
  suber.lastdelivered = -1
  suber.state = Running
  createThread(suber.thread, run, suber)

proc newSuber*[TData; SuberMaxTopics: static int](
 cachemaxcapacity = 10000000, cachelength = 1000000, maxdelivery = -1, channelsize = 200): Suber[TData, SuberMaxTopics] =
  result = Suber[TData, SuberMaxTopics]()
  initSuber(result, nil, nil, cachemaxcapacity, cachelength, maxdelivery, channelsize)

proc setPushCallback*[TData](suber: Suber, onPush: PushCallback[TData]) =
  suber.pushCallback = onPush

proc setDeliverCallback*[TData](suber: Suber, onDeliver: DeliverCallback[TData]) =
  suber.deliverCallback = onDeliver

proc setPullCallback*[TData](suber: Suber, onPull: PullCallback[TData]) =
  suber.pullCallback = onPull

proc stop*[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics]) =
  ## | 1: refuses to accept new messages
  ## | 2: returns the processing thread for joining
  ## | 3: all already accepted messages (in the channel buffer) are processed to cache (may trigger deliveries)
  ## | 4: executes one final delivery, if undelivered messages exist in cache
  ## | 5: processing loop stops, thread is stopped and joined with caller
  if suber.thread.running:
    suber.state = Stopping
    suber.channel.send(SuberMessage[TData](kind: smNil))
  joinThread(suber.thread)

proc stopImmediately*[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics]) =
  ## instantly stops the service
  suber.state = InstantStop
  if suber.thread.running: suber.channel.send(SuberMessage[TData](kind: smNil))

proc getChannelQueueLengths*(suber: Suber): (int, int, int) {.inline.} =
  ## Reports amounts of buffered messages in channel queue for monitoring and backpressure purposes:
  ## | first field: Current number of messages in the channel buffer
  ## | second field: Peak number of queued messages since queue was empty
  ## | third field: Maximum number of queued messages ever
  (suber.channel.peek(), suber.peakchannelqueuelength, suber.maxchannelqueuelength)

proc getChannelQueueSize*(suber: Suber): int = suber.channelqueuesize

# topics ----------------------------------------------------

proc addTopic*(suber: Suber, topic: Topic | int): bool {.discardable.} =
  ## Adds new topic.
  ## 
  ## Returns false if maximum number of topics is already added.
  var newsetref = new IntSet
  let (index , res) = suber.subscribers.insert(topic, newsetref)
  if res:
    suber.subscribers.withFound(topic, index): value[][] = initIntSet()
  return res
  
proc removeTopic*(suber: Suber, topic: Topic | int) =
  suber.subscribers.del(topic)
  suber.topicexpirations.del(topic)

proc hasTopic*(suber: Suber, topic: Topic | int): bool =
  not (findIndex(suber.subscribers, topic) == NotInStash)

proc getTopiccount*(suber: Suber): int = suber.subscribers.len

proc getSubscribersbytopic*(suber: Suber): seq[tuple[id: Topic; subscribers: IntSet]] =
  ## Reports subscribers for each topic.
  for (topicid , index) in suber.subscribers.keys():
    suber.subscribers.withFound(topicid, index):
      result.add((topicid, value[][]))

# subscribe ----------------------------------------------------

proc subscribe*(suber: Suber, subscriber: Subscriber, topic: Topic | int; createnewtopic = false): bool {.discardable.} =
  ## Creates new subscription. If `createnewtopic` is false, the topic must already exist,
  ## otherwise it is added as needed.
  withValue(suber.subscribers, topic):
    value[][].incl(int(subscriber))
    return true
  do:
    if not createnewtopic: return false
    var newsetref = new IntSet
    let (index , res) = suber.subscribers.insert(topic, newsetref)
    if res:
      suber.subscribers.withFound(topic, index):
        value[][] = initIntSet()
        value[][].incl(int(subscriber))
    return res
    
proc unsubscribe*(suber: Suber, subscriber: Subscriber, topic: Topic) =
  ## Removes a subscription.
  suber.subscribers.withValue(Topic(topic)): value[][].excl(int(subscriber))

proc removeSubscriber*(suber: Suber, subscriber: Subscriber) =
  ## Removes all subscriptions of the subscriber.
  for (topicid , index) in suber.subscribers.keys():
    suber.subscribers.withFound(topicid, index): value[][].excl(int(subscriber))
        
proc getSubscriptions*(suber: Suber, subscriber: Subscriber): seq[Topic] =
  for (topic , index) in suber.subscribers.keys():
    suber.subscribers.withFound(topic, index):
      if value[][].contains(int(subscriber)): result.add(topic)

proc getSubscribers*(suber: Suber, topic: Topic | int): IntSet =
  suber.subscribers.withValue(topic): return value[][]

proc getSubscribers*(suber: Suber): IntSet =
  for (topic , index) in suber.subscribers.keys():
    suber.subscribers.withFound(topic, index): result.incl(value[][])
      
proc getSubscribers*(suber: Suber, topics: openArray[Topic]): IntSet =
  ## Gets subscribers that are subscribing to any of the topics (set union).
  for topic in topics:
    suber.subscribers.withValue(topic): result.incl(value[][])

proc getSubscribers*(suber: Suber, message: ptr SuberMessage, toset: var IntSet, clear = true) =
  ## Gets subscribers to given message into the `toset` given as parameter.
  ## If `clear` is true, the `toset` is cleared first.
  if clear: toset.clear()
  for topic in message.topics.items():
    suber.subscribers.withValue(Topic(topic)): toset.incl(value[][])

proc isSubscriber*(suber: Suber, subscriber: Subscriber, topic: Topic): bool =
  suber.subscribers.withValue(topic): return value[][].contains(int(subscriber))

template testSubscriber() =
  suber.subscribers.withValue(topic):
    if not value[][].contains(int(subscriber)): return false
  do: return false

proc isSubscriber*(suber: Suber, subscriber: Subscriber, topics: openArray[Topic]): bool =
  for topic in topics: testSubscriber()
  true

proc isSubscriber*(suber: Suber, subscriber: Subscriber, topics: IntSet): bool =
  for topic in topics.items(): testSubscriber()
  true

# deliver ------------------------------------------------

proc doDelivery*[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics]) =
  ## Call this to trigger a delivery (DeliverCallback).
  ## To achieve time-based delivery, call this on regular intervals.
  if likely(suber.deliverCallback != nil and suber.state == Running):
    suber.channel.send(SuberMessage[TData](kind: smDeliver))

template handleDelivery() =
  if likely(suber.deliverCallback != nil):
    var messages: seq[ptr SuberMessage[TData]]
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

proc push*[TData](suber: Suber, topics: sink IntSet, data: sink TData, size: int = 0) =
  ## Pushes new message to message cache to be delivered.
  ## | `suber`: the service
  ## | `topics`: set of topics that this message belongs to
  ## | `data`: message payload. Available later as the data field of SuberMessage
  ## | `size`: Size of the data. Required only when size-based delivery is being used.
  if(unlikely) topics.len == 0: return
  if (likely) suber.state == Running: suber.channel.send(SuberMessage[TData](kind: smMessage, topics: move topics, data: move data, size: size))

proc push*[TData](suber: Suber, topic: Topic | int, data: sink TData, size: int = 0) =
  var topicset = initIntSet()
  topicset.incl(int(topic))
  if (likely) suber.state == Running: suber.channel.send(SuberMessage[TData](kind: smMessage, topics: move topicset, data: move data, size: size))

template evictCache() =
  var current = suber.head + 1
  if(unlikely) current == suber.cache.len: current = 0
  var evictedsize = 0
  while evictedsize < message.size:
    if suber.cache[current].kind == smMessage:
      for topic in suber.cache[current].topics:
        suber.topicexpirations[topic] = suber.cache[current].timestamp
      evictedsize += suber.cache[current].size
      `=destroy`(suber.cache[current])
      suber.cache[current] = SuberMessage[TData](kind: smNil)
    current.inc
    if current == suber.cache.len: current = 0
    if (unlikely) current == suber.head: break
  suber.cachesize -= evictedsize

template handlePush() =
  message.timestamp = getMonoTime()

  if(unlikely) suber.cachesize + message.size > suber.CacheMaxCapacity:
    if suber.deliverCallback != nil: handleDelivery()
    evictCache()
  
  if suber.deliverCallback != nil:
    if ((suber.head == suber.CacheMaxLength - 1 and suber.lastdelivered < 1) or suber.lastdelivered == suber.head + 1): handleDelivery()

  suber.head.inc
  suber.cachesize += message.size

  if suber.cache.len >= suber.CacheMaxLength:
    if (unlikely) suber.head == suber.CacheMaxLength: suber.head = 0
    if suber.cache[suber.head].kind == smMessage:
      suber.cachesize -= suber.cache[suber.head].size
      for topic in suber.cache[suber.head].topics:
        suber.topicexpirations[topic] = suber.cache[suber.head].timestamp
    suber.cache[suber.head] = move message
  else: suber.cache.add(message)

  if suber.pushCallback != nil: suber.pushCallback(addr suber.cache[suber.head])

  if suber.deliverCallback != nil and suber.MaxDelivery > -1:
    let deliverysize =
      if suber.lastdelivered < suber.head: suber.head - suber.lastdelivered
      else: suber.CacheMaxLength - (suber.head - suber.lastdelivered)
    if deliverysize >= suber.MaxDelivery: handleDelivery()

# pull ----------------------------------------------------

{.push hints:off.}
proc pull*[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics], subscriber: Subscriber | int, topics: sink IntSet, aftertimestamp: sink MonoTime): bool {.discardable.} =
  ## Requests messages after given timestamp and belonging to certain topics.
  ## | `suber`: service
  ## | `subscriber`: will be passed to callback
  ## | `topics`: set of topics that are of interest
  ## | `aftertimestamp`: only messages published after this timestamp will be pulled
  ## 
  ## Returns false, if there was nothing to pull.
  assert(suber.pullCallback != nil, "call setPullCallback before pull")
  if (unlikely) suber.state != Running: return false
  if(unlikely) suber.head == -1 or topics.len == 0: return false
  for topic in topics:
    suber.subscribers.withValue(Topic(topic)):
      if not value[][].contains(int(subscriber)): return false
    do:
      return false
  when subscriber is int:
    suber.channel.send(SuberMessage[TData](kind: smPull, subscriber: Subscriber(subscriber),
     pulltopics: move topics, aftertimestamp: move aftertimestamp))
  else:
    suber.channel.send(SuberMessage[TData](kind: smPull, subscriber: subscriber,
     pulltopics: move topics, aftertimestamp: move aftertimestamp))
  return true
{.pop.}

proc pull*[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics], subscriber: Subscriber | int, topic: Topic | int, aftertimestamp: sink MonoTime): bool {.discardable.} =
  var topicset = initIntSet()
  topicset.incl(int(topic))
  return pull(suber, subscriber, topicset, aftertimestamp)

proc pullAll*[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics], subscriber: Subscriber | int, aftertimestamp: sink MonoTime): bool {.discardable.} =
  var topicset = initIntSet()
  for (topic , index) in suber.subscribers.keys():
    suber.subscribers.withFound(topic, index):
      if value[][].contains(int(subscriber)):  topicset.incl(int(topic))
  return pull(suber, subscriber, topicset, aftertimestamp)

template handlePull() =
  var expiredtopics: seq[Topic]
  var messages: seq[ptr SuberMessage[TData]]
   
  var remainingtopics = initIntSet()
  for topic in message.pulltopics.items():
    let expiration = suber.topicexpirations.getOrDefault(topic)
    if expiration > message.aftertimestamp: expiredtopics.add(topic)
    else: remainingtopics.incl(topic)

  var current = suber.head + 1
  var wrapped = false
  while true:
    current.dec
    if (unlikely) wrapped and current == suber.head: break
    if (unlikely) current == -1:
      if suber.head == suber.cache.len - 1: break
      current = suber.cache.len ; wrapped = true; continue
    if (unlikely) suber.cache[current].kind != smMessage: break
    for topic in remainingtopics.items():
      if not suber.cache[current].topics.contains(topic): continue
      if suber.cache[current].timestamp > message.aftertimestamp:
        messages.add(addr suber.cache[current])
        break
      else:
        remainingtopics.excl(topic)
        if remainingtopics.len == 0:
          wrapped = true
          current = suber.head + 1
  suber.pullcallback(message.subscriber, expiredtopics, messages)

# find ---------------------------------------------------

proc find*[TData](suber: Suber, query: MonoTime,
 callback: proc(query: MonoTime, message: ptr SuberMessage[TData]) {.gcsafe, raises:[].}) =
  ## Calls the `callback` with (pointer to) the `message` that has the timestamp given in `query`.
  ## 
  ## **Important**: If queried message is not cached, message will be `nil`. Always check for `nil` first.
  ## 
  ## Note that callback must be gcsafe and not raise any exceptions.
  suber.channel.send(SuberMessage[TData](kind: smFind, findtimestamp: query, findcallback: callback))

template doBinarysearch(first, last: int, found: ptr SuberMessage) =
  while true:
    if last < first: break
    let avg = (first + last) div 2
    if suber.cache[avg].timestamp == message.findtimestamp:
      (found = addr suber.cache[avg] ; break)
    if suber.cache[avg].timestamp < message.findtimestamp: first = avg + 1
    else: last = avg - 1

template handleFind() =
  var found: ptr SuberMessage[TData]
  if(likely) suber.head > -1:
    var oldest = suber.head + 1
    if suber.cache.len < suber.CacheMaxLength: oldest = 0
    else:
      if(unlikely) oldest == suber.cache.len: oldest = 0
      while suber.cache[oldest].kind != smMessage: # always at least 1 message in cache
        oldest.inc
        if(unlikely) oldest == suber.cache.len: oldest = 0
    
    if message.findtimestamp >= suber.cache[oldest].timestamp:
      var first, last: int
      if oldest <= suber.head: (first = oldest ; last = suber.head)
      elif message.findtimestamp >= suber.cache[0].timestamp: (first = 0 ; last = suber.head)
      else: (first = suber.head ; last = suber.CacheMaxLength - 1)
      doBinarysearch(first, last, found)
  message.findcallback(message.findtimestamp, found)

# sync ----------------------------------------------------

proc doSynced*[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics], callback: proc() {.gcsafe, raises:[].}) =
  ## Calls the `callback` so that no other callbacks (delivery, etc.) are not being processed in parallel
  ##
  ## Note that callback must be gcsafe and not raise any exceptions.
  suber.channel.send(SuberMessage[TData](kind: smSync, syncedcallback: callback))

template handleSync() =
  message.syncedcallback()

# run ----------------------------------------------------

proc run[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics]) {.thread, nimcall.} =
  while true:
    var message = suber.channel.recv()
    let channelqueuelength = suber.channel.peek()
    if channelqueuelength == 0:
      suber.peakchannelqueuelength = 0
      if(unlikely) suber.state == Stopping:
        suber.channel.close()
        handleDelivery()
        break
    else:
      if(unlikely) channelqueuelength > suber.peakchannelqueuelength:
        suber.peakchannelqueuelength = channelqueuelength
        if(unlikely) channelqueuelength > suber.maxchannelqueuelength:
          suber.maxchannelqueuelength = channelqueuelength  
    case message.kind
      of smPull: handlePull()
      of smMessage: handlePush()
      of smDeliver: handleDelivery()
      of smFind: handleFind()
      of smSync: handleSync()
      of smNil:
        if suber.state == InstantStop or suber.channel.peek() == 0:
          suber.channel.close()
          break
{.pop.}