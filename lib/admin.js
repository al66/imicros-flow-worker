/**
 * @license MIT, imicros.de (c) 2018 Andreas Leinen
 */
"use strict";

const { Kafka, logLevel } = require("kafkajs");
const { v4: uuid } = require("uuid");
const _ = require("lodash");

module.exports = {
    name: "admin",
    
    /**
     * Service settings
     */
    settings: {
        /*
        brokers: ["localhost:9092"],
        numPartitions: 10
        */
    },

    /**
     * Service metadata
     */
    metadata: {},

    /**
     * Service dependencies
     */
    dependencies: [],	

    /**
     * Actions
     */
    actions: {
        
        registerService: {
            acl: "before",
            params: {
                serviceId: { type: "uuid" }
            },
            async handler(ctx) {
                let owner = _.get(ctx,"meta.ownerId",null);
                
                let topic = `service-${owner}${ctx.params.serviceId}`;
                let topics = [{
                    topic: topic,
                    numPartitions: this.numPartitions,      // default: 10
                    replicationFactor: 1                    // default 1
                }];
                this.logger.info("Create topic ", topic);
                try {
                    await this.admin.createTopics({
                        // validateOnly: true,  // default false
                        waitForLeaders: false,  // default true
                        timeout: 1000,          // default: 1000 (ms)
                        topics: topics,
                    });
                } catch (err) {
                    if (err.error === "Topic with this name already exists") return { topic: topic };
                    return { topic: topic, error: err };
                }
                return { topic: topic };
            }
        }, 
        
        unregisterService: {
            acl: "before",
            params: {
                serviceId: { type: "uuid" }
            },
            async handler(ctx) {
                let owner = _.get(ctx,"meta.ownerId",null);
                
                let topic = `service-${owner}${ctx.params.serviceId}`;
                this.logger.info("Delete topic ", topic);
                try {
                    await this.admin.deleteTopics({
                        topics: [topic],
                        timeout: 5000               // default: 5000 (ms)
                    });
                } catch (err) {
                    return { topic: topic, error: err };
                }
                return { topic: topic };
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
    methods: {},
	
    /**
     * Service created lifecycle event handler
     */
    created() {
        
        this.clientId = this.name + uuid(); 
        this.brokers = this.settings.brokers /* istanbul ignore next */ || ["localhost:9092"];
        
        // serviceLogger = kafkaLogLevel => ({ namespace, level, label, log }) ...
        this.serviceLogger = () => ({ level, log }) => {
            switch(level) {
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
            }
        };
        
        this.numPartitions = this.settings.numPartitions || 10;
        
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

    },

    /**
     * Service started lifecycle event handler
     */
    async started() {
        
        this.admin = await this.kafka.admin();
        await this.admin.connect();
        this.logger.info("Admin client connected to kafka brokers " + this.brokers.join(","));
        
    },

    /**
     * Service stopped lifecycle event handler
     */
    async stopped() {
        
        await this.admin.disconnect();
        this.logger.info("Admin client disconnected");
        
    }
};