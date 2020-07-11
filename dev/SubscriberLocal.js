"use strict";
const { ServiceBroker } = require("moleculer");
const { Publisher } = require("../index");
const { Admin } = require("../index");
const { Subscriber } = require("../index");

const { v4: uuid } = require("uuid");

const timestamp = Date.now();
let topic = `performance-topic-${timestamp}`;
const serviceId = uuid();
const ownerId = uuid();


let broker  = new ServiceBroker({
    nodeID: "Broker" + timestamp,
    logger: console,
    logLevel: "info", // "error", //"debug"
});

let n = 50000;
let p = 10000;
let s = 1;
let count = 0;
let offset;
let emit = async () => {
    ts = Date.now();
    let opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, groupId: `g-${timestamp}`, access: [`g-${timestamp}`] } };
    //let params;

    for (let i = 0; i < n; i += p) {
        let ts = Date.now();
        let jobs = Array.from(Array(p),(x,i) => { return {
            event: "user.created",
            payload: { msg: "Number" + i, offset: i }
        };
        });
        await Promise.all(jobs.map(async (params) => await broker.call("publisher.emit", params, opts)));
        let te = Date.now();
        console.log({
            "sent": p,
            "time (ms)": te-ts
        });
        count += p;
    }
    te = Date.now();
    offset = calls.length;
    console.log({
        "emit completed": {
            "produced": count,
            "time (ms)": te-ts
        }
    });
};
const calls = [];
let consumercount = {};
let ts, te, tf;
let run = async () => {
    
    // Create new topic first
    await broker.createService(Admin, Object.assign({ settings: { brokers: ["192.168.2.124:9092"] } }));
    await broker.start();
    let opts = { meta: { user: { id: `1-${timestamp}` , email: `1-${timestamp}@host.com` }, ownerId: ownerId, access: [`g-${timestamp}`] } };
    let params = {
        serviceId: serviceId
    };
    let res = await broker.call("admin.registerService", params, opts);
    if (res.error) console.log(res.error);
    topic = res.topic;
    
    // Produce queue entries
    await broker.createService(Publisher, Object.assign({ settings: { 
        brokers: ["192.168.2.124:9092"], 
        topic: topic 
    }}));
    await broker.waitForServices(["publisher"]);
    await emit();

    let groupId = "local";
    await broker.createService(Subscriber,Object.assign({ 
        // name: `${this.consumerPrefix}${ctx.params.consumerId}`,
        settings: { 
            brokers: ["192.168.2.124:9092"], 
            topic: topic, 
            groupId: groupId, 
            fromBeginning: true
        }
    }));
    await broker.waitForServices(["subscriber"]);
    
    // start consuming
    let process = async function () {

        let consumerId = "local";
        let ts = Date.now();
        let n = 0;
        let err = false;
        while ( calls.length < count && n < count + 100 && !err) {
            let fetch = async function () {
                try {
                    let msg = await broker.call("subscriber.fetch");
                    if (msg && msg.error) {
                        console.log("Fetch returned error " + consumerId + ": " + msg.error);
                        // err = true;
                    } else if (msg) {
                        calls.push(msg);
                        if (!consumercount[consumerId]) consumercount[consumerId] = 0;  
                        consumercount[consumerId]++; 
                        await broker.call("subscriber.commit");
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
    await broker.stop();
    
};
run();
