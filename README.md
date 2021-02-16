# Suber

An in-process multi-threading non-blocking topic-based loosely-coupled generic publish/subscribe engine with microsecond-scale performance supporting delivery:
- when a message is published
- when triggered (e.g. timer-based periodical delivery)
- when a given number new messages are available for delivery
- when message cache size is about to exceed some threshold
- when undelivered messages are about to be evicted from cache
- when client requests (e.g. pull-based delivery or specific redelivery for state syncing)

## Documentation

http://htmlpreview.github.io/?https://github.com/olliNiinivaara/Suber/blob/master/doc/suber.html

## Installation

`nimble install https://github.com/olliNiinivaara/Suber`