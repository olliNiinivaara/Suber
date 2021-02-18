# Suber

An in-process multi-threading non-blocking topic-based loosely-coupled generic publish/subscribe engine with microsecond-scale performance supporting delivery:
- when a message is published
- when triggered (e.g. timer-based periodical delivery)
- when a given number of new messages are available for delivery
- when message cache size is about to exceed some threshold
- when some undelivered messages are about to be evicted from cache
- when client requests (e.g. pull-based delivery or specific redelivery for state syncing)

## Documentation

http://htmlpreview.github.io/?https://github.com/olliNiinivaara/Suber/blob/master/doc/suber.html

## Installation

`nimble install https://github.com/olliNiinivaara/Suber`

## Example

```nim
import suber

template topic(name: string): Topic = (if name == "Cats": 1.Topic else: 2.Topic)
template subscriber(name: string): Subscriber =
  if name == "Amy": 1.Subscriber else: 2.Subscriber

let bus = newSuber[string, 2]()
bus.addTopic(topic "Cats")
bus.addTopic(topic "Dogs" )
bus.subscribe(subscriber "Amy", topic "Cats")
bus.subscribe(subscriber "Bob", topic "Dogs")

bus.setDeliverCallback(proc(messages: openArray[ptr SuberMessage[string]]) = (
  block:  
    var subscriberids: IntSet
    for message in messages:
      echo message.data
      {.gcsafe.}: bus.getSubscribers(message, subscriberids)
      for subscriberid in subscriberids: echo " to ", subscriberid
))

bus.push(topic "Cats", "cat-related message")
bus.push(topic "Dogs", "dog-related message")
bus.push(toIntSet([topic "Cats", topic "Dogs"]), "multitopical message")

bus.doDelivery()
jointhreads bus.stop()
```
