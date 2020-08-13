## version 0.2.1

* using mongo-driver-sync
* accepting only connection string instead of set of servers addresses
* do not stop executor on queue stop
* fail tolerance on passing packet to consumer

## version 0.1.3

* reduce mongo operations count on default settings
* ability to stop queue

## version 0.1.2

* use existing mongodb connection (for spring)

## version 0.1.1

* anonymous connection
* listener calls bind to fetch limit

## version 0.1.0

* fixed index issue with mongo 4.2
* updated all libs to latest versions
* tests moved to testcontainers
* fixed app startup freeze on non-empty queue

## version 0.0.3 (ru.infon.oss)

initial release

* using mongo 3.0