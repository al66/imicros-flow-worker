/**
 * @license MIT, imicros.de (c) 2018 Andreas Leinen
 */
"use strict";

const { Kafka, logLevel } = require("kafkajs");
const { v4: uuid } = require("uuid");
const _ = require("lodash");
const { Serializer } = require("./serializer/base");

module.exports = {
    name: "publisher",
    
	/**
	 * Service settings
	 */
    settings: {
        /*
        brokers: ["localhost:9092"]
        topic: "events"
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

        emit: {
            params: {
                event: { type: "string" },
                payload: { type: "any" }
            },
            async handler(ctx) {
                
                // set meta
                let meta = ctx.meta;
                // set key: 
                //      1) ownerId 
                //      2) user.id 
                //      3) "core"  (DEFAULT_KEY)
                let key = _.get(meta,"user.id","core");
                key = _.get(meta,"ownerId",key);
                
                // clean up meta
                meta = _.omit(meta, ["acl","auth","token","accessToken","serviceToken"]);
                
                // create message
                let content = {
                    event: ctx.params.event,
                    payload: await this.serializer.serialize(ctx.params.payload),
                    meta: await this.serializer.serialize(meta),
                    version: "1.0.0",
                    uid: uuid(),
                    timestamp: Date.now()
                };
                let msg = {
                    value: JSON.stringify(content)
                };
                
                // Emit event
                try {
                    await this.send(msg);
                    this.logger.debug("Event emitted", { topic: this.topic, event: content.event, uid: content.uid, timestamp: content.timestamp, version: content.version });
                    return { topic: this.topic, event: content.event, uid: content.uid, timestamp: content.timestamp, version: content.version };
                } catch (err) {
                    this.logger.error(`Failed to emit event ${content.event} to topic ${this.topic}`, { content: content, error: err });
                    throw err;
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

        async sendBatch (last) {
            if (!this.commit.length) return;
            let current = this.commit[this.commit.length - 1].timestamp;
            if (current > last) return;

            let batch = [];
            let pick = this.commit.length > this.maxBatchSize ? this.maxBatchSize : this.commit.length;
            for (let i = 0; i < pick; i++) batch.push(this.commit.shift());
            let messages = [];
            batch.map(e => messages.push(e.msg));
            // console.log({ sent: pick, current: current, last: last });
            try {
                await this.producer.send({
                    topic: this.topic,
                    messages: messages
                });
                // console.log({ sent: messages.length });
                // console.log("Event emitted", { topic: topic, event: content.event, uid: content.uid, timestamp: content.timestamp, version: content.version });
                batch.forEach(e => e.resolve(e.index));
                return;
            } catch (err) {
                this.logger.error(`Failed to send messages to topic ${this.topic}`, { error: err });
                throw err;
            }

        },

        async send (msg) {
            let received = Date.now();
            let p = new Promise(resolve => this.commit.push({ timestamp: received, msg: msg, resolve: resolve}));

            if (this.commit.length >= this.maxBatchSize) {
                this.sendBatch();
            } else {
                setTimeout(function () { this.sendBatch(received); }.bind(this), 50);
            }
            return p;
        }
        
    },

    /**
     * Service created lifecycle event handler
     */
    created() {
        
        this.clientId = this.name + uuid(); 
        this.brokers = this.settings.brokers || ["localhost:9092"];
        
        // serviceLogger = kafkaLogLevel => ({ namespace, level, label, log }) ...
        this.serviceLogger = () => ({ level, log }) => {
            switch(level) {
                /* istanbul ignore next */
                case logLevel.ERROR:
                    return this.logger.error("namespace:" + log.message, log);
                /* istanbul ignore next */
                case logLevel.WARN:
                    return this.logger.warn("namespace:" + log.message, log);
                /* istanbul ignore next */
                case logLevel.INFO:
                    return this.logger.info("namespace:" + log.message, log);
                /* istanbul ignore next */
                case logLevel.DEBUG:
                    return this.logger.debug("namespace:" + log.message, log);
                /* istanbul ignore next */
                case logLevel.NOTHING:
                    return this.logger.debug("namespace:" + log.message, log);
            }
        };
        
        this.defaults = {
            connectionTimeout: 1000,
            retry: {
                initialRetryTime: 100,
                retries: 8
            },
            maxBatchSize: 1000
        };
        
        // Create the client with the broker list
        this.kafka = new Kafka({
            clientId: this.clientId,
            brokers: this.brokers,
            logLevel: 5, //logLevel.DEBUG,
            logCreator: this.serviceLogger,
            ssl: this.settings.ssl || null,     // refer to kafkajs documentation
            sasl: this.settings.sasl || null,   // refer to kafkajs documentation
            connectionTimeout: this.settings.connectionTimeout ||  this.defaults.connectionTimeout,
            retry: this.settings.retry || this.defaults.retry
        });

        this.topic = this.settings.topic || "events";
        this.maxBatchSize = ( this.settings && this.settings.maxBatchSize ) ? this.settings.maxBatchSize : this.defaults.maxBatchSize;
        this.commit = [];
        
        this.serializer = new Serializer();
    },

	/**
	 * Service started lifecycle event handler
	 */
    async started() {
        
        this.producer = await this.kafka.producer({ allowAutoTopicCreation: true });
        await this.producer.connect();
        this.logger.info("Producer connected to kafka brokers " + this.brokers.join(",") + " - Topic: " + this.topic);
        
        
    },

	/**
	 * Service stopped lifecycle event handler
	 */
    async stopped() {
        
        await this.producer.disconnect();
        this.logger.info("Producer disconnected");
        
    }
};