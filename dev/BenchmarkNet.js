"use strict";
const { ServiceBroker } = require("moleculer");

const timestamp = Date.now();

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

let queue = 0;

let Producer = {
    name: "producer",
    actions: {
        send: {
            async handler(ctx) {
                // queue.push({ item: ctx.params.number });
                queue++;
                return { numer: ctx.params.number };
            } 
        }
    } 
};
let Broker = {
    name: "broker",
    actions: {
        fetch: {
            async handler() {
                let item = { item: queue-- };
                // let item = queue.shift();
                return item;
            } 
        },
        commit: {
            async handler() {
                return { commit: true };
            } 
        }
    }
};
let Consumer = {
    name: "consumer",
    actions: {
        fetch: {
            async handler(ctx) {
                return await ctx.call("broker.fetch",{},{});
            } 
        },
        commit: {
            async handler(ctx) {
                return await ctx.call("broker.commit",{},{});
            } 
        }
    } 
};

let n = 1000000;
let nConsumer = 1;
let count = 0;
let calls = 0;

let ts, te, tf;
let run = async () => {

    await broker.createService(Producer);
    await broker.createService(Broker);
    await broker.createService(Consumer);
    await broker.start();
    await broker.waitForServices(["broker", "consumer", "producer"]);
    
    
    // Produce queue entries
    let produce = async function (number) {
        await broker.call("producer.send", { number: number }, {});
        count++;
    };
    let jobs = Array.from(Array(n),(x,index) => index + 1);
    ts = Date.now();
    await Promise.all(jobs.map(j => produce(j)));
    jobs = null;
    te = Date.now();
    console.log({
        "produced": count,
        "time (ms)": te-ts
    });
    
    // start external consumers
    let consumers = Array.from(Array(nConsumer),(x,index) => index + 1);

    // start fetching items 
    let process = async function (consumer) {

        let ts = Date.now();
        
        let fetched = 0;
        while (calls < count) {
        // while (calls.length < count) {
            let fetch = async function () {
                try {
                    let msg = await broker.call("consumer.fetch", {});
                    if (msg) {
                        // calls.push(msg);
                        calls++;
                        let ack = await broker.call("consumer.commit", {});
                        if (!ack) console.log("Failed to commit - call failed");
                        fetched++;
                        return ack;
                    }
                    console.log("Fetch failed (Null) ");
                    return { error: "nothing received" };
                } catch (err) {
                    console.log("Fetch failed (Error) ");
                    return { error: err };
                }
            };
            await fetch();
            // if (calls.length >= count) console.log("Received complete"); 
            if (calls >= count) console.log("Received complete"); 
        }
        
        let te = Date.now();
        console.log({
            "fetched": fetched,
            "time (ms)": te-ts
        });
        
    };
    ts = Date.now();
    await Promise.all(consumers.map(async (consumer) => await process(consumer)));
    tf = Date.now();
    console.log({
        "handler completed": {
            "events emitted": count,
            "handler calls": calls,
            // "handler calls": calls.length,
            // "handler offset": calls[calls.length-1] ? calls[calls.length-1].offset : "undefined",
            "time (ms)": tf-ts
        }
    });
    
    // clean up
    await broker.stop();
    
};
run();
