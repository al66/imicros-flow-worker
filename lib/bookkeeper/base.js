/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 */
"use strict";

const Redis = require("ioredis");

const Base  = {
    
    /**
     * Methods
     */
    methods: {

        async nextIndex ({ owner, serviceId }) {
            let key = `${ owner }-${ serviceId }`;
            try {
                let index = await this.client.hincrby(key,"INDEX",1);
                if (index && index > 0) return index;
                throw new Error("Failed to retrieve new index");
            } catch (err) {
                this.logger.error("Failed to retrieve new index", err);
                return null;
            }
        },
        
        async fetchNext ({ owner, serviceId, workerId, timeToRecover }) {
            let key = `${ owner }-${ serviceId }`;
            let set = await this.client.hgetall(key);
            // send again
            if (set[workerId]) return set[workerId].split(":")[0];
            // check for missing acknowlegde && recover
            if (timeToRecover) {
                let recover;
                await Promise.all(Object.keys(set).map(async (att) => {
                    if (att !== "INDEX" && att !== "FETCHED") {
                        let vals = set[att].split(":");
                        if (vals.length === 2) {
                            let time = parseInt(vals[1]) + parseInt(timeToRecover);
                            if (time < Date.now()) recover = await this.client.recover(key, att, workerId);
                        }
                    }
                }));
                if (recover) return recover.split(":")[0];
            }
            // fetch next
            let next = await this.client.fetch(key, workerId, Date.now());
            if (next) return next.split(":")[0];
            return 0;
        },
        
        async ack ({ owner, serviceId, workerId }) {
            let key = `${ owner }-${ serviceId }`;
            return this.client.hdel(key, workerId);
        },
        
        async info ({ owner, serviceId }) {
            let key = `${ owner }-${ serviceId }`;
            let set = await this.client.hgetall(key);
            return set;
        },
        
        connectRedis () {
            return new Promise((resolve, reject) => {
                /* istanbul ignore else */
                let redisOptions = this.settings.redis || {};   // w/o settings the client uses defaults: 127.0.0.1:6379
                this.client = new Redis(redisOptions);

                this.client.on("connect", (() => {
                    this.logger.info("Connected to Redis");
                    
                    // fetch - must be atomic transaction
                    let lua = "";
                    lua += "if redis.call('HEXISTS', KEYS[1], 'FETCHED') == 1 then ";
                    lua += "  local index = tonumber(redis.call('HGET', KEYS[1], 'INDEX')) ";
                    lua += "  local fetched = tonumber(redis.call('HGET', KEYS[1], 'FETCHED')) ";
                    lua += "  if fetched >= index then ";
                    lua += "    return 0 ";
                    lua += "  else ";
                    lua += "    local index = redis.call('HINCRBY', KEYS[1], 'FETCHED', 1) ";
                    lua += "    redis.call('HSET', KEYS[1], KEYS[2], index .. ':' .. ARGV[1]) ";
                    lua += "    return redis.call('HGET', KEYS[1], KEYS[2]) ";
                    lua += "  end ";
                    lua += "else ";
                    lua += "  local index = redis.call('HINCRBY', KEYS[1], 'FETCHED', 1) ";
                    lua += "  redis.call('HSET', KEYS[1], KEYS[2], index .. ':' .. ARGV[1]) ";
                    lua += "  return redis.call('HGET', KEYS[1], KEYS[2]) ";
                    lua += "end ";
                    this.client.defineCommand("fetch", {
                        numberOfKeys: 2,
                        lua
                    });
                    
                    // recover - should be atomic transaction
                    lua = "";
                    lua += "if redis.call('HEXISTS', KEYS[1], KEYS[2]) == 1 then ";
                    lua += "  local entry = redis.call('HGET', KEYS[1], KEYS[2]) ";
                    lua += "  redis.call('HSET', KEYS[1], ARGV[1], entry) ";
                    lua += "  redis.call('HDEL', KEYS[1], KEYS[2]) ";
                    lua += "  return redis.call('HGET', KEYS[1], ARGV[1]) ";
                    lua += "else ";
                    lua += "  return 0 ";
                    lua += "end ";
                    this.client.defineCommand("recover", {
                        numberOfKeys: 2,
                        lua
                    });
                    
                    this.connected = true;
                    resolve();
                }).bind(this));

                this.client.on("close", (() => {
                    this.connected = false;
                    this.logger.info("Disconnected from Redis");
                }).bind(this));

                /* istanbul ignore next */
                this.client.on("error", ((err) => {
                    this.logger.error("Redis redis error", err.message);
                    this.logger.debug(err);
                    /* istanbul ignore else */
                    if (!this.connected) reject(err);
                }).bind(this));
            });
        },   

        disconnectRedis () {
            return new Promise((resolve) => {
                /* istanbul ignore else */
                if (this.client && this.connected) {
                    this.client.on("close", () => {
                        resolve();
                    });
                    this.client.disconnect();
                } else {
                    resolve();
                }
            });
        }
    },
    
    /**
     * Service started lifecycle event handler
     */
    async started() {

        // connect to db
        await this.connectRedis();
        
    },

    /**
     * Service stopped lifecycle event handler
     */
    async stopped() {
        
        // disconnect from db
        await this.disconnectRedis();
        
    }
    
};

module.exports = {
    Bookkeeper: Base
};