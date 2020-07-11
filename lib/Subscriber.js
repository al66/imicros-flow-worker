/**
 * @license MIT, imicros.de (c) 2018 Andreas Leinen
 */
"use strict";

const { Kafka, logLevel } = require("kafkajs");
const { v4: uuid } = require("uuid");
const _ = require("lodash");
const { Serializer } = require("./serializer/base");

module.exports = {
    name: "subscriber",
    
    /**
     * Service settings
     */
    settings: {
        /*
        brokers: ["localhost:9092"]
        topic: "events",
        groupId: "flow",
        fromBeginning: false,
        handler: "service.action"
        */
    },

    /**
     * Service metadata
     */
    metadata: {},

    /**
     * Service dependencies
     */
    //dependencies: [],	

    /**
     * Actions
     */
    actions: {
        fetch: {
            params: {
          //
            },
            async handler(/* ctx */) {
                let fetch = async function () {
                    if (this.claim.length > 0) {
                        this.logger.info("Fetched same item again", { topic: this.claim[0].topic, partition: this.claim[0].partition, offset: this.claim[0].offset });
                        let content = this.claim[0];
                        content.resolve = null;
                        return content; 
                    }
                    if (this.queue.length < 1) {
                        return { error: "empty queue - queue:" + this.queue.length + " claim:" + this.claim.length};
                    }
                    let claim = this.queue.shift();
                    
                    // build return item
                    let content;
                    try {
                        content = JSON.parse(claim.value.toString());
                        content.topic = claim.topic,
                        content.partition = claim.partition,
                        content.offset = claim.offset,
                        content.payload = await this.serializer.deserialize(content.payload);
                        content.meta = await this.serializer.deserialize(content.meta);
                        content.resolve = claim.resolve;
                    } catch (err) {
                        this.logger.info("Failed to fetch item", { topic: claim.topic, partition: claim.partition, offset: claim.offset });
                        return { error: "failed to read item in queue" };
                    }
                  
                    this.claim.push(content);
                    this.logger.debug("Current queue", { queue: this.queue.length, claim: content });
                    return content;
                }.bind(this);
                if (this.queue.length < 1) {
                    // wait some milliseconds
                    return await new Promise((resolve) => {
                        this.logger.info("Delay 50 ms", { 
                            subscriber: this.name 
                        });
                        setTimeout(async () => resolve(await fetch()), 50);
                    });
                } else {
                    return await fetch();
                }
            }
        },
        commit: {
            async handler(/* ctx */) {
                try {
                    let item = this.claim.shift();
                    if (!item) {
                        this.logger.info("Failed commit - no item in claim");
                        return { error: "no item in claim" };
                    }
                    item.resolve(item.offset);
                    this.offset[item.partition] = parseInt(item.offset);
                    this.logger.debug("Commit", { service: this.name, topic: item.topic, partition: item.partition, offset: item.offset });
                    return { partition: item.partition, offset: item.offset };
                } catch (err) {
                    this.logger.info("Commit error", err);
                    return { error: "failed to resolve offset" };
                }
            }
        }
    },

    /**
     * Events
     */
    events: {},

    /**
     * Methods
     */
    methods: {
        
        /**
         * Subscribe 
         *     - Starts a consumer for the subscription 
         * 
         * @param {Object} subscription 
         * 
         */
        async subscribe (subscription) {
            try {
                // create consumer
                let consumer = this.kafka.consumer({ 
                    groupId: subscription.groupId,
                    allowAutoTopicCreation: true   
                });

                // connect consumer and subscribe to the topic
                await consumer.connect();

                // memorize consumer for cleaning up on service stop
                this.consumer = consumer;
                await this.consumer.subscribe({ 
                    topic: subscription.topic, 
                    fromBeginning: subscription.fromBeginning 
                });
                // don't know how to set offset ... better to start always with "fromBeginning"...consuming is quite cheap
                //await this.consumer.seek({ topic: subscription.topic, partition: 0, offset: 0 })

                this.logger.info(`Subscription for topic '${subscription.topic}' starting`, { subscription: subscription });
                // start runner
                let service = this;
                this.consumer.run({
                    eachBatchAutoResolve: false,
                    autoCommit: true,
                    autoCommitThreshold: 100,
                    partitionsConsumedConcurrently: 1,  // default: 1
                    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
                            
                        // initilize partition in offset store, if not defined
                        if (typeof service.offset[batch.partition] === "undefined") service.offset[batch.partition] = -1;
                        if (typeof service.queuedOffset[batch.partition] === "undefined") service.queuedOffset[batch.partition] = -1;
                        
                        // get last queued offset of current partition
                        let queuedOffset = service.queuedOffset[batch.partition];
                        
                        let queued = false;
                        for (let msg of batch.messages) {
                            if (!isRunning() || isStale()) break;

                            let offset = parseInt(msg.offset);
                            if (offset > queuedOffset) {
                                msg.topic = batch.topic;
                                msg.partition = batch.partition;
                                msg.resolve = resolveOffset;
                                service.queue.push(msg);
                                service.queuedOffset[batch.partition] = offset;
                                queued = true;
                            }
                        }
                        if (queued) {
                            service.logger.debug("Received", { 
                                partition: batch.partition, 
                                batch: batch.messages.length, 
                                from: batch.messages[0].offset,
                                to: batch.messages[batch.messages.length -1].offset,
                                high: batch.highWatermark,
                                queue: service.queue.length
                            });
                        } else {
                            service.logger.debug("EachBatch idle", { 
                                current: service.offset[batch.partition],
                                queuedOffset: service.queuedOffset[batch.partition],
                                batch: batch.messages.length, 
                                from: batch.messages[0].offset,
                                to: batch.messages[batch.messages.length -1].offset
                            });
                        }
                        await heartbeat();
                        await commitOffsetsIfNecessary();
                        
                    }               
                });

                this.logger.info(`Subscription for topic '${subscription.topic}' running`, { subscription: subscription });

            } catch (e) {
				/* istanbul ignore next */
                this.logger.warn(`Subscription for topic ${subscription.topic}) failed`);
				/* istanbul ignore next */
                throw e;
            }
        },

        /**
         * processMessage
         *      - Calls the event handler 
         * 
         * @param {Object} message 
         * @param {Object} subscription 
         * 
         * @returns {Boolean} result
         */
        async processMessage(message, subscription) {
            let offset = message.offset.toString();
            let topic = subscription.topic;
            try {

                let content = JSON.parse(message.value.toString());
                let params = {}, options ={ meta: {} };
 
                this.logger.debug(`Event topic ${topic} offset ${offset} received`, {
                    subscription: subscription,
                    value: content
                });

                try {
                    params =  _.omit(content,["meta"]);
                    params.offset =  offset;
                    params.payload = await this.serializer.deserialize(content.payload);
                    options = {
                        meta: await this.serializer.deserialize(content.meta)
                    };
                } catch(err) {
                    //
                }
                
                /* 
                 * call the given handler of subscription
                 */
                if (subscription.handler && params ) {

                    // wait this.broker.call(subscription.handler, params, options);
                    this.logger.info(`Event topic ${topic} offset ${offset} handler called`, {
                        groupId: subscription.groupId,
                        event: content.event,
                        handler: subscription.handler,
                        uid: content.uid,
                        timestamp: content.timestamp,
                        meta: options.meta
                    });
                }

            } catch(err) {
                switch (err.constructor) {
                    default: {
                        this.logger.error(`Unreadable event in topic ${topic} offset ${offset}`, err);
                        return Promise.reject(err);
                    }
                }
            }            
        },
		
        /**
         * log 
         *      - map kafkajs log to service logger 
         * 
         * @param {String} namespace 
         * @param {Object} level 
         * @param {String} label 
         * @param {Object} log 
         * 
         */
        log({ namespace, level, log }) {
            if (this.stopped) return;
            switch(level) {
				/* istanbul ignore next */
                case logLevel.ERROR:
                    return this.logger.error("KAFKAJS: " + namespace + log.message, log);
				/* istanbul ignore next */
                case logLevel.WARN:
                    return this.logger.warn("KAFKAJS: " + namespace + log.message, log);
                case logLevel.INFO:
                    return this.logger.info("KAFKAJS: " + namespace + log.message, log);
                case logLevel.DEBUG:
                    return this.logger.debug("KAFKAJS: " + namespace + log.message, log);
				/* istanbul ignore next */
                case logLevel.NOTHING:
                    return this.logger.debug("KAFKAJS: " + namespace + log.message, log);
            }
        }
        
    },

    /**
     * Service created lifecycle event handler
     */
    created() {
        
        this.clientId = this.name; 
        this.brokers = this.settings.brokers || ["localhost:9092"];

        this.defaults = {
            connectionTimeout: 2000,  // 1000
            retry: {
                initialRetryTime: 100, // 100
                retries: 8
            }
        };
        // Create the client with the broker list
        this.kafka = new Kafka({
            clientId: this.clientId,
            brokers: this.brokers,
            logLevel: 5,                        //logLevel.DEBUG,
            logCreator: () => this.log,			// serviceLogger = kafkaLogLevel => ({ namespace, level, label, log }) ...
            ssl: this.settings.ssl || null,     // refer to kafkajs documentation
            sasl: this.settings.sasl || null,   // refer to kafkajs documentation
            connectionTimeout: this.settings.connectionTimeout ||  this.defaults.connectionTimeout,
            retry: this.settings.retry || this.defaults.retry
        });

        this.topic = this.settings.topic || "events";
        this.subscription = {
            topic: this.settings.topic || "events",
            groupId: this.settings.groupId || uuid(),
            fromBeginning: this.settings.fromBeginning || false
        };
        this.consumer = null;

        this.serializer = new Serializer();
      
        this.queue = [];
        this.claim = [];
        this.offset = [];
        this.queuedOffset = [];
        
    },

    /**
     * Service started lifecycle event handler
     */
    async started() {
        
        // Start consumer
        await this.subscribe(this.subscription);

    },

    /**
     * Service stopped lifecycle event handler
     */
    async stopped() {

        this.stopped = true;
        if (this.consumer) {
            try {
                await this.consumer.stop();
                await this.consumer.disconnect();
                this.logger.info("Consumer disconnected");
            } catch (err) {
				/* istanbul ignore next */
                this.logger.error("Failed to disconnect consumer", err);
            }
        }
    
    }

};