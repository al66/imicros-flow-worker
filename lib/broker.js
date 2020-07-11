/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 */
"use strict";

const Subscriber = require("./Subscriber");

module.exports = {
    name: "broker",
    
    /**
     * Service settings
     */
    settings: {},

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
    actions: {},

    /**
     * Events
     */
    events: {
        "consumer.requested": {
            group: "broker",
            async handler(ctx) {
                let consumer = Subscriber;
                await ctx.broker.createService(consumer,Object.assign({ 
                    name: `${this.consumerPrefix}${ctx.params.consumerId}`,
                    settings: { 
                        brokers: ["192.168.2.124:9092"], 
                        topic: ctx.params.topic, 
                        groupId: ctx.params.groupId, 
                        fromBeginning: true
                    }
                }));
            }
        }
    },

    /**
     * Methods
     */
    methods: {},

    /**
     * Service created lifecycle event handler
     */
    created() {
        this.consumerPrefix = "broker.";
    },

    /**
     * Service started lifecycle event handler
     */
    async started() {},

    /**
     * Service stopped lifecycle event handler
     */
    async stopped() {}

};