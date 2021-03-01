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
##  {.gcsafe.}:
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
##  proc redeliver() =
##    {.gcsafe.}:
##      while true:
##        if stop: break else: (sleep(2000+rand(3000)) ; if stop: break)
##        let since = getMonoTime() - initDuration(milliseconds = 500 + rand(1500))
##        echo "" ; echo "--" ; echo("Chas requests Nim-related news since timestamp @", since)
##        let subscriber = subscribers.find("Chas")
##        bus.pull(subscriber, toIntSet([topics.find("Nim")]), since, proc(subscriber: Subscriber,
##         expiredtopics: openArray[Topic], messages: openArray[ptr SuberMessage[string]]) = (
##          block:
##            {.gcsafe.}:
##              if expiredtopics.len > 0: echo "Sorry, not stocking that old news anymore."
##              else: writeMessages(int(subscriber), messages)))
##  
##  proc onDeliver(messages: openArray[ptr SuberMessage[string]]) {.gcsafe, raises:[].} =
##    {.gcsafe.}: writeMessages(-1, messages)
##  
##  bus.setDeliverCallback(onDeliver)
##  for i in 0 ..< 4: (for j in 0 .. i: bus.subscribe(i.Subscriber, j.Topic, true))
##  
##  var delivererthread: Thread[void]
##  createThread(delivererthread, deliver)
##  var redelivererthread: Thread[void]
##  createThread(redelivererthread, redeliver)
##  var publisherthreads: array[4, Thread[int]]
##  for i in 0 ..< 4: createThread(publisherthreads[i], generateMessages, i)
##  joinThreads publisherthreads ; joinThread bus.stop()
##  joinThreads([delivererthread, redelivererthread]) ; echo ""
##

when not compileOption("threads"): {.fatal: "Suber requires threads:on compiler option".}
when not defined(gcDestructors): {.fatal: "Suber requires gc:arc or orc compiler option".}
  
import intsets, tables, std/monotimes, stashtable
export intsets, tables, monotimes

type
  SuberError* = object of CatchableError
  Topic* = distinct int
  Subscriber* = distinct int
    
  SuberMessageKind = enum
    smNil,
    smMessage,
    smDeliver,
    smPull
    smFind

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
        pullcallback: PullCallback[TData]
      of smFind:
        findtimestamp: MonoTime
        #TODO: findlatestwithtopic: Topic
        findcallback: proc(query: MonoTime, message: ptr SuberMessage[TData]) {.gcsafe, raises:[].}
      else: discard

    PullCallback*[TData] = proc(subscriber: Subscriber, expiredtopics: openArray[Topic],
     messages: openArray[ptr SuberMessage[TData]]) {.gcsafe, raises:[].}

else:
  type
    SuberMessage*[TData] = object
      ## push, pull, deliver and find -callbacks will return (pointers to) SuberMessages.
      topics*: IntSet ## The `Topic`s that this message belongs to
      timestamp*: MonoTime ## Unique timestamp that orders and identifies messages
      data*: TData ## Message payload, a generic type
      size: int ## Size of the data, used to trigger size-based deliveries

    PullCallback*[TData] = proc(subscriber: Subscriber, expiredtopics: openArray[Topic],
     messages: openArray[ptr SuberMessage[TData]]) {.gcsafe, raises:[].}
      ## Give PullCallback -type proc as a parameter to `pull` proc, and it will be called back.
      ## | `subscriber` identifies the puller (pass-through parameter given in pull proc)
      ## | `expiredtopics` is list of topics for which all messages are not anymore cached
      ## | `messages` includes results for topics that had all requested messages still in cache
      ## 
      ## Note that PullCallback must be gcsafe and not raise any exceptions.

type
  DeliverCallback*[TData] = proc(messages: openArray[ptr SuberMessage[TData]]) {.gcsafe, raises:[].}
    ## Proc that gets called when there are messages to be delivered. Set this in `newSuber` or with `setDeliverCallback`.
    ## 
    ## Note that DeliverCallback must be gcsafe and not raise any exceptions.
  
  PushCallback*[TData] = proc(message: ptr SuberMessage[TData]) {.gcsafe, raises:[].}
    ## Proc that gets called **every** time when a new message is published.
    ## Set this in `newSuber` or with `setPushCallback`.
    ## 
    ## Note that PushCallback must be gcsafe and not raise any exceptions.

when not defined(nimdoc):
  type Suber*[TData; SuberMaxTopics: static int] = ref object
    CacheMaxCapacity: int
    CacheMaxLength: int 
    MaxDelivery: int
    cache: seq[SuberMessage[TData]]
    cachesize: int
    head: int
    lastdelivered: int
    deliverCallback: DeliverCallback[TData]
    pushCallback: PushCallback[TData]
    subscribers: StashTable[Topic, IntSet, SuberMaxTopics]
    channel: Channel[SuberMessage[TData]]
    thread: Thread[Suber[TData, SuberMaxTopics]]
    peakchannelqueuelength: int
    maxchannelqueuelength: int
    drainbeforestop: bool
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
{.push hint[ConvFromXtoItselfNotNeeded]: off.}

proc `==`*(x, y: Topic): bool {.borrow.}
proc `==`*(x, y: Subscriber): bool {.borrow.}

proc toMonoTime*(i: int64): MonoTime {.inline.} =
  result = cast[MonoTime](i)

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
  suber.subscribers = newStashTable[Topic, IntSet, SuberMaxTopics]()
  suber.channel.open(channelsize)
  suber.pushCallback = onPush
  suber.deliverCallback = onDeliver    
  suber.cache = newSeqOfCap[SuberMessage[TData]](suber.CacheMaxLength)
  suber.head = -1
  suber.lastdelivered = -1
  createThread(suber.thread, run, suber)

proc newSuber*[TData; SuberMaxTopics](onPush: PushCallback[TData], onDeliver: DeliverCallback[TData],
 cachemaxcapacity = 10000000, cachelength = 1000000, maxdelivery = -1, channelsize = 200): Suber[TData, SuberMaxTopics] =
  ## Creates new Suber service.
  ## | `TData`: define type of message data
  ## | `SuberMaxTopics`: set the maximum number of topics (million topics is ok, but requires more memory to be reserved)
  ## | `cachemaxcapacity`: If sum of cached SuberMessage sizes would exceed this, oldest messages are delivered and removed to make room
  ## | `cachelength`: Maximum number of messages in the cache, before oldest messages are delivered and removed to make room
  ## | `maxdelivery`: If number of undelivered messages exceeds this, messages get delivered (-1 means unlimited delivery)
  ## | `channelsize`: Number of messages that may get buffered before `push` will block. For unlimited queue, set channelsize to 0.
  result = Suber[TData, SuberMaxTopics]()
  initSuber(result, onPush, onDeliver, cachemaxcapacity, cachelength, maxdelivery, channelsize)

proc newSuber*[TData; SuberMaxTopics](onDeliver: DeliverCallback[TData],
 cachemaxcapacity = 10000000, cachelength = 1000000, maxdelivery = -1, channelsize = 200): Suber[TData, SuberMaxTopics] =
  result = Suber[TData, SuberMaxTopics]()
  initSuber(result, nil, onDeliver, cachemaxcapacity, cachelength, maxdelivery, channelsize)

proc newSuber*[TData; SuberMaxTopics](
 cachemaxcapacity = 10000000, cachelength = 1000000, maxdelivery = -1, channelsize = 200): Suber[TData, SuberMaxTopics] =
  result = Suber[TData, SuberMaxTopics]()
  initSuber(result, nil, nil, cachemaxcapacity, cachelength, maxdelivery, channelsize)

proc setPushCallback*[TData](suber: Suber, onPush: PushCallback[TData]) =
  suber.pushCallback = onPush

proc setDeliverCallback*[TData](suber: Suber, onDeliver: DeliverCallback[TData]) =
  suber.deliverCallback = onDeliver

proc stop*[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics]): Thread[Suber[TData, SuberMaxTopics]] =
  ## Stops the service, but first waits that all buffered messages get processed.
  if not suber.thread.running: return suber.thread
  suber.drainbeforestop = true
  suber.channel.send(SuberMessage[TData](kind: smNil))
  return suber.thread

proc stopImmediately*[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics]) =
  if suber.thread.running: suber.channel.send(SuberMessage[TData](kind: smNil))

proc getChannelQueueLengths*(suber: Suber): (int, int, int) =
  ## Reports amounts of buffered messages in channel queue for monitoring and backpressure purposes:
  ## | first field: Current number of messages in the channel buffer
  ## | second field: Peak number of queued messages since queue was empty
  ## | third field: Maximum number of queued messages ever
  (suber.channel.peek(), suber.peakchannelqueuelength, suber.maxchannelqueuelength)
  
# topics ----------------------------------------------------

proc addTopic*(suber: Suber, topic: Topic | int) =
  ## Adds new topic.
  ## 
  ## Will raise `SuberError`, if maximum number of topics is already added.
  if suber.subscribers.insert(Topic(topic), initIntSet())[0] == NotInStash:
    raise newException(SuberError, "SuberMaxTopics already in use")
  
proc removeTopic*(suber: Suber, topic: Topic | int) =
  suber.subscribers.del(topic)

proc hasTopic*(suber: Suber, topic: Topic | int): bool =
  not (findIndex(suber.subscribers, topic) == NotInStash)

proc getTopiccount*(suber: Suber): int = suber.subscribers.len

proc getSubscribersbytopic*[Topic](suber: Suber): seq[tuple[id: Topic; subscribers: IntSet]] =
  ## Reports subscribers for each topic.
  for (topicid , index) in suber.subscribers.keys():
    suber.subscribers.withFound(topicid, index):
      result.add((topicid, value))

# subscribe ----------------------------------------------------

proc subscribe*(suber: Suber, subscriber: Subscriber, topic: Topic | int; createnewtopic = false): bool {.discardable.} =
  ## Creates new subscription. If `createnewtopic` is false, the topic must already exist,
  ## otherwise it is added as needed.
  withValue(suber.subscribers, Topic(topic)):
    value[].incl(int(subscriber))
    return true
  do:  
    if not createnewtopic: return false
    var newset = initIntSet()
    newset.incl(int(subscriber))
    return suber.subscribers.insert(Topic(topic), newset)[1]
    
proc unsubscribe*(suber: Suber, subscriber: Subscriber, topic: Topic) =
  ## Removes a subscription.
  suber.subscribers.withValue(topic): value[].excl(int(subscriber))

proc removeSubscriber*(suber: Suber, subscriber: Subscriber) =
  ## Removes all subscriptions of the subscriber.
  for (topicid , index) in suber.subscribers.keys():
    suber.subscribers.withFound(topicid, index): value[].excl(int(subscriber))
        
proc getSubscriptions*(suber: Suber, subscriber: Subscriber): seq[Topic] =
  for (topic , index) in suber.subscribers.keys():
    suber.subscribers.withFound(topic, index):
      if value[].contains(int(subscriber)): result.add(topic)

proc getSubscribers*(suber: Suber, topic: Topic | int): IntSet =
  suber.subscribers.withValue(topic): return value[]

proc getSubscribers*(suber: Suber): IntSet =
  for (topic , index) in suber.subscribers.keys():
    suber.subscribers.withFound(topic, index): result.incl(value[])
      
proc getSubscribers*(suber: Suber, topics: openArray[Topic]): IntSet =
  ## Gets subscribers that are subscribing to any of the topics (set union).
  for topic in topics:
    suber.subscribers.withValue(topic): result.incl(value[])

proc getSubscribers*(suber: Suber, message: ptr SuberMessage, toset: var IntSet, clear = true) =
  ## Gets subscribers to given message into the `toset` given as parameter.
  ## If `clear` is true, the `toset` is cleared first.
  if clear: toset.clear()
  for topic in message.topics.items():
    suber.subscribers.withValue(Topic(topic)): toset.incl(value[])

proc isSubscriber*(suber: Suber, subscriber: Subscriber, topic: Topic): bool =
  suber.subscribers.withValue(topic): return value[].contains(subscriber)

# deliver ------------------------------------------------

proc doDelivery*[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics]) =
  ## Call this to trigger a delivery (DeliverCallback).
  ## To achieve time-based delivery, call this on regular intervals.
  if unlikely(suber.deliverCallback == nil): raise newException(SuberError, "deliverCallback not set")
  suber.channel.send(SuberMessage[TData](kind: smDeliver))

template handleDelivery() =
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
  suber.channel.send(SuberMessage[TData](kind: smMessage, topics: move topics, data: move data, size: size))

proc push*[TData](suber: Suber, topic: Topic | int, data: sink TData, size: int = 0) =
  var topicset = initIntSet()
  topicset.incl(int(topic))
  suber.channel.send(SuberMessage[TData](kind: smMessage, topics: move topicset, data: move data, size: size))

template evictCache() =
  var current = suber.head + 1
  if(unlikely) current == suber.cache.len: current = 0
  var evictedsize = 0
  while evictedsize < message.size: # TODO: we could evict more, now that we are at it
    if suber.cache[current].kind == smMessage:
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
    elif suber.MaxDelivery > -1:
      let deliverysize =
        if suber.lastdelivered < suber.head: suber.head - suber.lastdelivered
        else: suber.CacheMaxLength - (suber.head - suber.lastdelivered)
      if deliverysize >= suber.MaxDelivery: handleDelivery()

  suber.head.inc
  suber.cachesize += message.size

  if suber.cache.len < suber.CacheMaxLength: suber.cache.add(message)
  else:
    if (unlikely) suber.head == suber.CacheMaxLength: suber.head = 0
    if suber.cache[suber.head].kind == smMessage:
      suber.cachesize -= suber.cache[suber.head].size
    suber.cache[suber.head] = message
  
  if suber.pushCallback != nil: suber.pushCallback(addr suber.cache[suber.head])

# pull ----------------------------------------------------

proc pull*[TData](suber: Suber, subscriber: Subscriber | int, topics: sink IntSet,
 aftertimestamp: sink MonoTime, callback: PullCallback[TData]) =
  ## Requests messages after given timestamp and belonging to certain topics.
  ## | `suber`: service
  ## | `subscriber`: will be passed to callback
  ## | `topics`: set of topics that are of interest
  ## | `aftertimestamp`: only messages published after this timestamp will be pulled
  ## | `callback`: the procedure that will receive the results of the pull
  if(unlikely) suber.head == -1 or topics.len == 0: return
  for topic in topics:
    suber.subscribers.withValue(Topic(topic)):
      if not value[].contains(subscriber): return
    do:
      return
  suber.channel.send(SuberMessage[TData](kind: smPull, subscriber: Subscriber(subscriber),
   pulltopics: move topics, aftertimestamp: move aftertimestamp, pullcallback: callback))

template handlePull() =
  var expiredtopics: seq[Topic]
  var messages: seq[ptr SuberMessage[TData]]
   
  var remainingtopics = initIntSet()
  for topic in message.pulltopics.items(): remainingtopics.incl(topic)
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
  message.pullcallback(message.subscriber, expiredtopics, messages)

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

# run ----------------------------------------------------

proc run[TData; SuberMaxTopics](suber: Suber[TData, SuberMaxTopics]) {.thread, nimcall.} =
  while true:
    var message = suber.channel.recv()
    let channelqueuelength = suber.channel.peek()
    if channelqueuelength == 0:
      suber.peakchannelqueuelength = 0
      if(unlikely) suber.drainbeforestop: break
    else:
      if(unlikely) channelqueuelength > suber.peakchannelqueuelength:
        suber.peakchannelqueuelength = channelqueuelength
        if(unlikely) channelqueuelength > suber.maxchannelqueuelength:
          suber.maxchannelqueuelength = channelqueuelength  
    case message.kind
      of smMessage: handlePush()
      of smDeliver: handleDelivery()
      of smPull: handlePull()
      of smFind: handleFind()
      of smNil:
        suber.channel.close()
        if suber.channel.peek() == 0 or not suber.drainbeforestop: break

{.pop.}
{.pop.}