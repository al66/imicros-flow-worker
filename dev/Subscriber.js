"use strict";
const { ServiceBroker } = require("moleculer");
const { Publisher } = require("../index");
const { Admin } = require("../index");
const { Broker } = require("../index");
const { Consumer } = require("../index");

const { v4: uuid } = require("uuid");

const timestamp = Date.now();
let topic = `performance-topic-${timestamp}`;
const serviceId = uuid();
const ownerId = uuid();

const transporter = "TCP";
// const transporter = "nats://192.168.2.124:4222";
// const transporter = "redis://192.168.2.124:6379";

let brokerConsumer  = new ServiceBroker({
    nodeID: "Consumer" + timestamp,
    logger: console,
    logLevel: "info", // "error", //"debug"
    transporter: transporter
});
let brokerAdmin  = new ServiceBroker({
    nodeID: "Admin" + timestamp,
    logger: console,
    logLevel: "error", //"debug"
    transporter: transporter
});
let brokerA  = new ServiceBroker({
    nodeID: "BrokerA" + timestamp,
    logger: console,
    logLevel: "info", // "error", //"debug"
    transporter: transporter
});
let brokerB  = new ServiceBroker({
    nodeID: "BrokerB" + timestamp,
    logger: console,
    logLevel: "info", // "error", //"debug"
    transporter: transporter
});
let brokerGateway  = new ServiceBroker({
    nodeID: "Gateway" + timestamp,
    logger: console,
    logLevel: "error", // "error", //"debug"
    transporter: transporter
});

let n = 10000;
let s = 1;
let count = 0;
let offset;
let emit = async () => {
    ts = Date.now();
    let opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, groupId: `g-${timestamp}`, access: [`g-${timestamp}`] } };
    //let params;

    let jobs = Array.from(Array(n),(x,i) => { return {
        event: "user.created",
        payload: { msg: "Number" + i, offset: i }
    };
    });
    await Promise.all(jobs.map(async (params) => await brokerConsumer.call("publisher.emit", params, opts)));

    /*
    for (let i = 1; i<=n; i++) {
        params = {
            event: "user.created",
            payload: { msg: "Number" + i, offset: i }
        };
        await brokerConsumer.call("publisher.emit", params, opts);
        count++;
    }
    */
    te = Date.now();
    offset = calls.length;
    count = jobs.length;
    console.log({
        "emit completed": {
            "produced": count,
            // "handler calls": offset,
            // "handler offset": calls[offset-1] ? calls[offset-1].params.offset : "undefined",
            "time (ms)": te-ts
        }
    });
};
const calls = [];
let consumercount = {};
let ts, te, tf;
let run = async () => {
    
    // Create new topic first
    await brokerAdmin.createService(Admin, Object.assign({ settings: { brokers: ["192.168.2.124:9092"] } }));
    await brokerAdmin.start();
    let opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, ownerId: ownerId, access: [`g-${timestamp}`] } };
    let params = {
        serviceId: serviceId
    };
    let res = await brokerAdmin.call("admin.registerService", params, opts);
    if (res.error) console.log(res.error);
    topic = res.topic;
    
    // Produce queue entries
    await brokerConsumer.createService(Publisher, Object.assign({ settings: { 
        brokers: ["192.168.2.124:9092"], 
        topic: topic 
    }}));
    await brokerConsumer.start();
    await emit();
    
    await brokerGateway.start();
    
    
    await brokerA.createService(Broker);
    await brokerA.start();
    await brokerB.createService(Broker);
    await brokerB.start();
    await brokerConsumer.createService(Consumer);

    // start consuming
    let process = async function () {

        let consumerId, consumerToken;
        // start consumer
        await brokerConsumer.waitForServices(["broker", "consumer"]).then(async () => {
            let res = await brokerConsumer.call("consumer.start", { groupId: ownerId, service: topic, serviceToken: "my service secret" }, {});
            consumerId = res.consumerId;
            consumerToken = res.consumerToken;
            if (!consumercount[consumerId]) consumercount[consumerId] = 0;  
        });
        await brokerGateway.waitForServices(["consumer"]);
        
        let ts = Date.now();
        let n = 0;
        let err = false;
        while ( calls.length < count && n < count + 100 && !err) {
            let fetch = async function () {
                let params;
                try {
                    params = {
                        consumerId: consumerId,
                        consumerToken: consumerToken
                    };
                    let msg = await  brokerGateway.call("consumer.fetch", params);
                    if (msg && msg.error) {
                        console.log("Fetch returned error " + consumerId + ": " + msg.error);
                        err = true;
                    } else if (msg) {
                        calls.push(msg);
                        if (!consumercount[consumerId]) consumercount[consumerId] = 0;  
                        consumercount[consumerId]++; 
                        params = {
                            consumerId: consumerId,
                            consumerToken: consumerToken,
                            task: {},
                            result: {}
                        };
                        await brokerGateway.call("consumer.commit", params);
                    } else {
                        console.log("Consumer Idle " + consumerId);
                    }
                } catch (err) {
                    console.log("Fetch failed", err);
                }
            };
            await fetch();
            n++;
        }
        let te = Date.now();
        console.log({
            "subscriber completed": {
                "fetched": consumercount[consumerId],
                "time (ms)": te-ts
            }
        });
    };
    ts = Date.now();
    let subscriptions = Array.from(Array(s),(x,i) => i);
    await Promise.all(subscriptions.map(() => process()));
    /*
    await Promise.all([
        process(),
        process(),
        process()
    ]);
    */
    // await process();
    /*
    await new Promise((resolve) => {
        let running = true;
        let timeout = setTimeout(() => {
            running = false;
            console.log("timeout");
            resolve();
        }, 5000);
        let loop = () => {
            if (!running) return;
            if (calls.length >= count) {
                clearTimeout(timeout);
                return resolve();
            }
            setImmediate(loop); 
        };
        loop();                
    });
    */
    tf = Date.now();
    offset = calls.length;
    console.log({
        "handler completed": {
            "events emitted": count,
            "handler calls": offset,
            "consumer count": consumercount,
            "handler offset": calls[offset-1] ? calls[offset-1].offset : "undefined",
            "time (ms)": tf-ts
        }
    });

    // clean up
    opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, ownerId: ownerId, access: [`g-${timestamp}`] } };
    params = {
        serviceId: serviceId
    };
    await brokerConsumer.stop();
    await brokerA.stop();
    await brokerB.stop();

    //res = await brokerAdmin.call("admin.unregisterService", params, opts);
    //if (res.error) console.log(res.error);
    
    await brokerAdmin.stop();
    await brokerGateway.stop();
    
};
run();
