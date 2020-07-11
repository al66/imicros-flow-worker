# imicros-flow-worker
[![Development Status](https://img.shields.io/badge/status-under_development-red)](https://img.shields.io/badge/status-under_development-red)

[Moleculer](https://github.com/moleculerjs/moleculer) service for a buffered work queue

This service is part of the imicros-flow engine and provides a buffered work queue for pulling messages by external services.

For use by the external service the following actions are provided:

- start { service, serviceToken } => { consumerId, consumerToken }
- stop { consumerId, consumerToken } => { done, error }
- fetch { consumerId, consumerToken } => { task }
- commit { consumerId, consumerToken } => { task, result }

via the moleculer-web gateway these actions can be easily published for external access as named pathes - e.g.:
/consumer/start
/consumer/{consumerId}/stop
/consumer/{consumerId}/fetch
/consumer/{consumerId}/commit

Multiple external consumers can work on the same service queue by an at least once guarantee. 
How many consumers can be started concurrently depends on the setting `numPartitions` for the creation of new kafka topics in the publisher service, as per partition only one consumer is allowed and further consumers would be in idle. 

When the external service calls the start action, the consumer service emits an `consumer.requested` event. One of the running broker services starts a new kafka consumer service with the name `<prefix>.consumerId`. A call of the to the fetch action of the consumer is then proxied to the `<prefix>.consumerId.fetch` action.


## Installation
```
$ npm install imicros-flow-worker --save
```
## Dependencies
Requires a running [Kafka](https://kafka.apache.org/) broker.

# Usage

## Usage consumer service
```js

```
## Actions (consumer service)
- start { service, serviceToken } => { consumerId, consumerToken }
- stop { consumerId, consumerToken } => { done | error }
- fetch { consumerId, consumerToken } => { task }
- commit { consumerId, consumerToken } => { task, result }
## Usage publisher service
```js

```
## Actions (publisher service)
- add { service, payload } => { topic, service, uid, timestamp, version }  
## Usage broker service
```js

```
The broker service provides no actions, it just create and start the subscriber service based on the event `consumer.requested` emitted by the consumer start action
## Usage broker service
```js

```
