"use strict";

const { ServiceBroker } = require("moleculer");
const { Bookkeeper } = require("../index");
const { v4: uuid } = require("uuid");

const owner = uuid();
const serviceId = uuid();

const Test = {
    name: "test",
    mixins: [Bookkeeper],
    settings: { 
        redis: {
            port: process.env.REDIS_PORT || 6379,
            host: process.env.REDIS_HOST || "127.0.0.1",
            password: process.env.REDIS_AUTH || "",
            db: process.env.REDIS_DB || 0,
        }
    },
    actions: {
        testNextIndex: {
            async handler() {
                let received = [];
                for (let i=0; i < 30; i++) {
                    received.push(await this.nextIndex({ owner, serviceId }));  
                }
                return received;
            }
        },
        testFetchFirst: {
            async handler() {
                let fetched = [];
                let workerId = uuid();
                for (let i=0; i < 5; i++) {
                    fetched.push(await this.fetchNext({ owner, serviceId, workerId }));
                }
                await this.ack({ owner, serviceId, workerId });
                for (let i=0; i < 5; i++) {
                    fetched.push(await this.fetchNext({ owner, serviceId, workerId }));
                }
                return fetched;
            }
        },
        testFetchAck: {
            async handler() {
                let fetched = [];
                let workerId = uuid();
                for (let i=0; i < 40; i++) {
                    fetched.push(await this.fetchNext({ owner, serviceId, workerId }));
                    await this.ack({ owner, serviceId, workerId });
                }
                return fetched;
            }
        },
        testFetchAndWrite: {
            async handler() {
                let workerA = uuid();
                let workerB = uuid();
                let self = this;
                async function fetch (workerId) {
                    let index = await self.fetchNext({ owner, serviceId, workerId });
                    await self.ack({ owner, serviceId, workerId });
                    return index;
                }
                
                let fetched = [];
                let writes =[];
                for (let n=0; n<20; n++) {
                    writes.push(this.nextIndex({ owner, serviceId }));
                    fetched.push(fetch(workerA));
                    fetched.push(fetch(workerB));
                }
                await Promise.all(writes);
                await Promise.all(fetched);
                
                let info = await this.info({ owner, serviceId });
                return info;
            }
        },
        testRecover: {
            async handler() {
                let workerId = uuid();
                // should fetch nothing
                await this.fetchNext({ owner, serviceId, workerId, timeToRecover: 10000 });
                await this.ack({ owner, serviceId, workerId });
                // should recover
                let result = await this.fetchNext({ owner, serviceId, workerId, timeToRecover: 10 });
                await this.ack({ owner, serviceId, workerId });
                console.log(await this.info({ owner, serviceId }));
                return result;
            }
        }

    }
};


describe("Test master/key service", () => {

    let broker, service;
    beforeAll(() => {
    });
    
    afterAll(() => {
    });

    beforeEach(() => {
    });
    
    describe("Test create service", () => {

        it("it should start the broker", async () => {
            broker = new ServiceBroker({
                logger: console,
                logLevel: "info" //"debug"
            });
            // Create service
            service = broker.createService(Test);
            // Start services
            await broker.start();
            expect(service).toBeDefined();
        });

    });

    describe("Test bookkeeper service", () => {

        it("it should receive 100 indices", async () => {
            return broker.call("test.testNextIndex").then(res => {
                expect(res).toBeDefined();
                expect(res.length).toEqual(30);
            });
        });
        
        it("it should fetch first two indices", async () => {
            return broker.call("test.testFetchFirst").then(res => {
                expect(res).toBeDefined();
                expect(res.length).toEqual(10);
                expect(res.filter(e => { return e === "1"; }).length).toEqual(5);
                expect(res.filter(e => { return e === "2"; }).length).toEqual(5);
                console.log(res);
            });
        });

        it("it should fetch and acknowlegde the rest", async () => {
            return broker.call("test.testFetchAck").then(res => {
                expect(res).toBeDefined();
                for (let i=3; i<=30; i++) {
                    expect(res.filter(e => { return e === `${ i }`; }).length).toEqual(1);
                }
                console.log(res);
            });
        });

        it("it should fetch and write", async () => {
            return broker.call("test.testFetchAndWrite").then(res => {
                expect(res).toBeDefined();
                expect(res.INDEX).toEqual("50");
                expect(res.FETCHED).toEqual("50");
                console.log(res);
            });
        });

        it("it should recover not acknowledged index 2", async () => {
            return broker.call("test.testRecover").then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual("2");
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