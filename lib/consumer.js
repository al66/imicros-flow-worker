/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 */
"use strict";

const { v4: uuid } = require("uuid");

module.exports = {
    name: "consumer",
    
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
    actions: {

        /**
         * start
         *      - start new consumer 
         * 
         * @param {String} groupId 
         * @param {String} service 
         * @param {String} serviceToken 
         * 
         * @returns {Object} { consumerId, consumerToken }
         */
        start: {
            params: {
                groupId: { type: "uuid" },
                service: { type: "string" },
                serviceToken:  { type: "string" }
            },
            async handler(ctx) {
                // request access for groupId with service & token and receive accessToken for group 
                // create consumerToken with encrypted accessToken
                let consumerToken = "";
                let consumerId = uuid();
                // topic = `${groupId}.${service}`
                // emit event consumer.requested
                ctx.emit("consumer.requested", { consumerId: consumerId, groupId: ctx.params.groupId, topic: ctx.params.service });
                // wait until running
                await ctx.broker.waitForServices([`${this.consumerPrefix}${consumerId}`]);
                return { consumerId: consumerId, consumerToken: consumerToken };
            }
        },
        
        /**
         * stop
         *      - stop consumer 
         * 
         * @param {String} consumerId 
         * @param {String} consumerToken 
         * 
         * @returns {Object} { done, error }
         */
        stop: {
            params: {
                consumerId: { type: "string" },
                consumerToken: { type: "string" }
            },
            async handler(/* ctx */) {
                // verify consumerToken
                // stop consumer and wait until stopped
                // return { done, error }
            }
        },
        
        /**
         * fetch
         *      - fetch item from consumer 
         * 
         * @param {String} consumerId 
         * @param {String} consumerToken 
         * 
         * @returns {Object} task
         */
        fetch: {
            params: {
                consumerId: { type: "string" },
                consumerToken: { type: "string" }
            },
            async handler(ctx) {
                // verify consumerToken
                // call service `${consumerId}.fetch` without params
                try {
                    let content = await ctx.call(`${this.consumerPrefix}${ctx.params.consumerId}.fetch`);
                    // decrypt accessToken from consumerToken
                    // call service context.get with key received in returned content
                    // build job
                    //  - task: returned content
                    //  - params: retrieved context key
                    // return job
                    if (!content) return { error: "nothing fetched" };
                    return content;
                } catch (err) {
                    return { error: err };
                }
            }
        },

        /**
         * commit
         *      - commit last fetched item
         * 
         * @param {String} consumerId 
         * @param {String} consumerToken
         * @param {Object|String} 
         * 
         * @returns {Object} { result, error }
         */
        commit: {
            params: {
                consumerId: { type: "string" },
                consumerToken: { type: "string" },
                task: { type: "object" },
                result: { type: "object" }
            },
            async handler(ctx) {
                // verify consumerToken
                // decrypt accessToken from consumerToken
                // save result to context key given in task
                // call service `${consumerId}.commit`
                return await ctx.call(`${this.consumerPrefix}${ctx.params.consumerId}.commit`);
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