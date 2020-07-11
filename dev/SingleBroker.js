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

let broker  = new ServiceBroker({
    nodeID: "Consumer" + timestamp,
    logger: console,
    logLevel: "info", // "error", //"debug"
    metrics: {
        enabled: false,
        reporter: [
            {
                type: "Console",
                options: {
                    // Printing interval in seconds
                    interval: 5,
                    // Custom logger.
                    logger: null,
                    // Using colors
                    colors: true,
                    // Prints only changed metrics, not the full list.
                    onlyChanges: true
                }
            }
        ]
    }    
});

let n = 10000;
let nConsumer = 3;
let count = 0;
let offset;

const calls = [];
let consumercount = {};
let ts, te, tf;
let run = async () => {

    await broker.createService(Admin, Object.assign({ 
        settings: { 
            brokers: ["192.168.2.124:9092"],
            numPartitions: 10
        } 
    }));
    await broker.start();
    
    // Create new topic first
    let opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, ownerId: ownerId, access: [`g-${timestamp}`] } };
    let params = {
        serviceId: serviceId
    };
    let res = await broker.call("admin.registerService", params, opts);
    if (res.error) console.log(res.error);
    topic = res.topic;

    // Start publisher, broker and consumer services
    await broker.createService(Publisher, Object.assign({ settings: { 
        brokers: ["192.168.2.124:9092"], 
        topic: topic 
    }}));
    await broker.createService(Broker);
    await broker.createService(Consumer);
    
    // Produce queue entries
    let produce = async function (number) {
        let opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, groupId: `g-${timestamp}`, access: [`g-${timestamp}`] } };
        let params = {
            event: "user.created",
            payload: { msg: "Number" + number, offset: number }
        };
        await broker.call("publisher.emit", params, opts);
        count++;
    };
    await broker.waitForServices(["broker", "consumer", "publisher"]);
    let jobs = Array.from(Array(n),(x,index) => index + 1);
    ts = Date.now();
    await Promise.all(jobs.map(j => produce(j)));
    te = Date.now();
    offset = calls.length;
    console.log({
        "produced": count,
        "time (ms)": te-ts
    });
    
    // start external consumers
    ts = Date.now();
    let starter = Array.from(Array(nConsumer),(x,index) => index + 1);
    let start = async function () {
        // start consumer
        let consumer = await broker.call("consumer.start", { groupId: ownerId, service: topic, serviceToken: "my service secret" }, {});
        if (typeof consumercount[consumer.consumerId] === "undefined") consumercount[consumer.consumerId] = 0;  
        return consumer;
    };
    let consumers = await Promise.all(starter.map(() => start()));

    // wait some stime
    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    await sleep(500);
    
    // start fetching items 
    let process = async function (consumer) {

        let ts = Date.now();
        
        let n = 0;
        let fetched = 0;
        // while ( (calls.length < count) && (n < count + 100)) {
        while (calls.length < count) {
            let fetch = async function () {
                try {
                    let params = {
                        consumerId: consumer.consumerId,
                        consumerToken: consumer.consumerToken
                    };
                    let msg = await broker.call("consumer.fetch", params);
                    if (msg && msg.error) {
                        console.log("Failed to fetch: " + msg.error + " " + consumer.consumerId);
                        return msg;   
                    }
                    if (msg) {
                        calls.push(msg);
                        if (!consumercount[consumer.consumerId]) consumercount[consumer.consumerId] = 0;  
                        consumercount[consumer.consumerId]++; 
                        let params = {
                            consumerId: consumer.consumerId,
                            consumerToken: consumer.consumerToken,
                            task: {},
                            result: {}
                        };
                        let ack = await broker.call("consumer.commit", params);
                        if (!ack) console.log("Failed to commit - call failed");
                        if (ack.error) console.log("Failed to commit: " + ack.error);
                        fetched++;
                        return ack;
                    }
                    console.log("Fetch failed (Null) " + consumer.consumerId);
                    return { error: "nothing received" };
                } catch (err) {
                    console.log("Fetch failed (Error) " + consumer.consumerId);
                    return { error: err };
                }
            };
            await fetch();
            if (calls.length >= count) console.log("Received complete"); 
            n++;
        }
        
        let te = Date.now();
        console.log({
            "fetched": fetched,
            "time (ms)": te-ts
        });
        
    };
    await Promise.all(consumers.map((consumer) => process(consumer)));
    // await process();
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
    let checkUnique = [];
    calls.map((e) => {
        if (!checkUnique[e.partition]) checkUnique[e.partition] = [];
        if (!checkUnique[e.partition][e.offset]) checkUnique[e.partition][e.offset] = 0;
        checkUnique[e.partition][e.offset]++;
        if (checkUnique[e.partition][e.offset] > 1) console.log(`Recieved double partition: ${e.partition} offset:${e.offset} count: ${checkUnique[e.partition][e.offset]}`);
    });

    // clean up
    opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, ownerId: ownerId, access: [`g-${timestamp}`] } };
    params = {
        serviceId: serviceId
    };
    await broker.stop();
    
};
run();
