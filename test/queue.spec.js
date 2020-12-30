"use strict";
const { ServiceBroker } = require("moleculer");
const { Queue } = require("../index");
//const { Bookkeeper } = require("../lib/bookkeeper/base");
const { Bookkeeper } = require("../index");
const { v4: uuid } = require("uuid");

const timestamp = Date.now();
const ownerId = `owner-${timestamp}`;
const serviceId= uuid();
const keys = {
    current: uuid(),
    previous: uuid()
};

// helper & mocks
const { Activity, activity } = require("./helper/activity");

const AclMock = {
    localAction(next, action) {
        return async function(ctx) {
            ctx.meta = Object.assign(ctx.meta,{
                ownerId: ownerId,
                acl: {
                    accessToken: "this is the access token",
                    ownerId: ownerId,
                    unrestricted: true
                },
                user: {
                    id: `1-${timestamp}` , 
                    email: `1-${timestamp}@host.com` 
                }
            });
            ctx.broker.logger.debug("ACL meta data has been set", { meta: ctx.meta, action: action });
            return next(ctx);
        };
    }    
};

// mock keys service
const KeysMock = {
    name: "keys",
    actions: {
        getOek: {
            handler(ctx) {
                if (!ctx.params || !ctx.params.service) throw new Error("Missing service name");
                if ( ctx.params.id == keys.previous ) {
                    return {
                        id: keys.previous,
                        key: "myPreviousSecret"
                    };    
                }
                return {
                    id: keys.current,
                    key: "mySecret"
                };
            }
        }
    }
};

// collect events
const collect = [];
const Collect = {
    name: "collect",
    events: {
        "**"(payload, sender, event, ctx) {
            collect.push({ payload, sender, event, ctx });
        }
    }
};

describe("Test context service", () => {

    let broker, service, opts, keyService;
    beforeAll(() => {
    });
    
    afterAll(async () => {
    });
    
    describe("Test create service", () => {

        it("it should start the broker", async () => {
            broker = new ServiceBroker({
                middlewares:  [AclMock],
                logger: console,
                logLevel: "info" //"debug"
            });
            keyService = await broker.createService(KeysMock);
            service = await broker.createService(Queue, Object.assign({ 
                mixins: [Bookkeeper],
                settings: { 
                    cassandra: {
                        contactPoints: process.env.CASSANDRA_CONTACTPOINTS || "127.0.0.1", 
                        datacenter: process.env.CASSANDRA_DATACENTER || "datacenter1", 
                        keyspace: process.env.CASSANDRA_KEYSPACE || "imicros_flow" 
                    },
                    redis: {
                        port: process.env.REDIS_PORT || 6379,
                        host: process.env.REDIS_HOST || "127.0.0.1",
                        password: process.env.REDIS_AUTH || "",
                        db: process.env.REDIS_DB || 0,
                    },
                    services: {
                        keys: "keys"
                    }
                },
                dependencies: ["keys"]
            }));
            await broker.createService(Collect);
            await broker.createService(Activity);
            await broker.start();
            expect(service).toBeDefined();
            expect(keyService).toBeDefined();
        });

    });
    
    describe("Test queue - single actions", () => {

        let workerA = "first worker", workerB = uuid();
        let token = {
            processId: uuid(),
            instanceId: uuid(),
            elementId: uuid(),
            type: "Service Task",
            status: "ACTIVITY.READY",
            user: {
                id: uuid()
            },
            ownerId: uuid(),
            attributes: { }
        };
        
        beforeEach(() => {
            opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, ownerId: `g-${timestamp}` } };
        });

        it("it should add a task ", () => {
            let params = {
                serviceId: serviceId,
                value: { msg: "say hello to the world" }
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        
        it("it should fetch a task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workerA,
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual({ msg: "say hello to the world" });
            });
        });
        
        it("it should fetch same task again ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workerA,
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual({ msg: "say hello to the world" });
            });
        });

        it("it should fetch nothing ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workerB,
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toEqual(null);
            });
        });

        it("it should acknowledge first task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workerA,
            };
            return broker.call("worker.queue.ack", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        
        it("it should fetch nothing ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workerA,
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toEqual(null);
            });
        });

        it("it should add a second task ", () => {
            let params = {
                serviceId: serviceId,
                value: { msg: "say hello again to the world" }
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        
        it("it should fetch the second task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workerA,
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual({ msg: "say hello again to the world" });
            });
        });
        
        it("it should recover the second task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workerB,
                timeToRecover: 1
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual({ msg: "say hello again to the world" });
            });
        });
        
        it("it should fetch nothing ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workerA,
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toEqual(null);
            });
        });

        it("it should retrieve queue info ", () => {
            let params = {
                serviceId: serviceId
            };
            return broker.call("worker.queue.info", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.INDEX).toEqual("2");
                expect(res.FETCHED).toEqual("2");
                console.log(res);
            });
        });

        it("it should add a task with a token", () => {
            let params = {
                serviceId: serviceId,
                value: { msg: "task with token" },
                token
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        
        it("it should fetch the task with token", () => {
            let params = {
                serviceId: serviceId,
                workerId: workerA,
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual({ msg: "task with token" });
            });
        });

        it("it should acknowledge the task with token and emit the token", () => {
            let params = {
                serviceId: serviceId,
                workerId: workerA,
                result: 5
            };
            return broker.call("worker.queue.ack", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
                expect(activity[0].params.token).toEqual(token);
                expect(activity[0].meta.ownerId).toEqual(ownerId);
                console.log(activity);
            });
        });
        
        it("it should rewind ", async () => {
            let params = {
                serviceId: serviceId,
                last: 0
            };
            await broker.call("worker.queue.rewind", params, opts);
            params = {
                serviceId: serviceId,
                workerId: workerA,
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual({ msg: "say hello to the world" });
            });
        });
        
        it("it should fetch null, if index doesn't exist ", async () => {
            let params = {
                serviceId: serviceId,
                last: -1
            };
            await broker.call("worker.queue.rewind", params, opts);
            params = {
                serviceId: serviceId,
                workerId: workerA,
            };
            await broker.call("worker.queue.ack", params, opts);
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toEqual(null);
            });
        });
        
    });

    describe("Test stop broker", () => {
        it("should stop the broker", async () => {
            expect.assertions(1);
            await broker.stop();
            expect(broker).toBeDefined();
        });
    });
    
});