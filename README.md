# imicros-flow-worker
[![Development Status](https://img.shields.io/badge/status-under_development-red)](https://img.shields.io/badge/status-under_development-red)

[Moleculer](https://github.com/moleculerjs/moleculer) service for a buffered work queue

This service is part of the imicros-flow engine and provides a buffered work queue for pulling messages by external services.

For use by the external service the following actions are provided:

- fetch { serviceId, workerId, timeToRecover } => value
- ack { serviceId, workerId } => true|false
- info { serviceId } => { INDEX, FETCHED, queued indices if not acknowledged }

via the moleculer-web gateway these actions can be easily published for external access as named pathes - e.g.:
```
 api/queue/{serviceId}/fetch
 api/queue/{serviceId}/info
```

Multiple external workers can work on the same service queue by an at least once guarantee. 

The tasks stored in cassandra are encrypted with a owner specific key (owner is set by the acl middleware).
For storing the current index and tracking the fetch/acknowledgements the Redis instance used by the bookkeeper must be persistent.

Calling "fetch" for a worker returns the next task. Calling "fetch" again will return the same task - until with call of "ack" for this worker the task is confirmed. Then the next "fetch" returns the next task.
If a task has not been confirmed by the worker after "timeToRecover" (default: 60s) has expired, it is delivered to the next worker. A different "timeToRecover" can be specified for each "fetch".

## Installation
```
$ npm install imicros-flow-worker --save
```
## Dependencies
Requires a running [Redis](https://redis.io/) instance for the bookkeeper.
Requires a running [Cassandra](https://cassandra.apache.org/) node/cluster for storing the tasks.
Requires broker middleware AclMiddleware or similar: [imicros-acl](https://github.com/al66/imicros-acl)
Reuires a running key server for retrieving the owner encryption keys: [imicros-keys](https://github.com/al66/imicros-keys)

# Usage

## Usage add
```js
let params = {
    serviceId: serviceId,
    value: { msg: "say hello to the world" }
};
let res = await broker.call("worker.queue.add", params, opts)
expect(res).toBeDefined();
expect(res).toEqual(true);

```
## Usage fetch
```js
let params = {
    serviceId: serviceId,
    workerId: "my external worker id"
};
let res = broker.call("worker.queue.fetch", params, opts)
expect(res).toBeDefined();
expect(res).toEqual({ msg: "say hello to the world" });

```
## Usage ack
```js
let params = {
    serviceId: serviceId,
    workerId: "my external worker id"
};
let res = broker.call("worker.queue.ack", params, opts)
expect(res).toBeDefined();
expect(res).toEqual(true);

```
## Usage info
```js
let params = {
    serviceId: serviceId,
    workerId: "my external worker id"
};
let res = broker.call("worker.queue.info", params, opts)
expect(res).toBeDefined();
expect(res.INDEX).toEqual("1");
expect(res.FETCHED).toEqual("1");

```
