/**
 * @license MIT, imicros.de (c) 2019 Andreas Leinen
 */
"use strict";

const Cassandra = require("cassandra-driver");
const crypto = require("crypto");
const { Serializer } = require("./serializer/base");

module.exports = {
    name: "worker.queue",
    
    /**
     * Service settings
     */
    settings: {
        /*
        cassandra: {
            contactPoints: ["192.168.2.124"],
            datacenter: "datacenter1",
            keyspace: "imicros_flow",
            contextTable: "context",
            instanceTable: "instances"
        },
        services: {
            keys: "keys"
        }
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
        
        /**
         * Add entry to queue 
         * 
         * @param {String} serviceId - uuid
         * @param {String} value     - task to be queued  
         * @param {String} token     - instance token  
         * 
         * @returns {Boolean} result
         */
        add: {
            acl: "before",
            params: {
                serviceId: { type: "uuid" },
                value: { type: "any" },
                token: { type: "object", optional: true }
            },
            async handler(ctx) {
                let ownerId = ctx?.meta?.ownerId ?? null;

                let oek;
                // get owner's encryption key
                try {
                    oek = await this.getKey({ ctx: ctx });
                } catch (err) {
                    throw new Error("failed to receive encryption keys");
                }
                
                let value = await this.serializer.serialize(ctx.params.value);                
                // encrypt value
                let iv = crypto.randomBytes(this.encryption.ivlen);
                try {
                    // hash encription key with iv
                    let key = crypto.pbkdf2Sync(oek.key, iv, this.encryption.iterations, this.encryption.keylen, this.encryption.digest);
                    // encrypt value
                    value = this.encrypt({ value: value, secret: key, iv: iv });
                } catch (err) {
                    this.logger.error("Failed to encrypt value", { 
                        error: err, 
                        iterations: this.encryption.iterations, 
                        keylen: this.encryption.keylen,
                        digest: this.encryption.digest
                    });
                    throw new Error("failed to encrypt");
                }
                
                // get next index
                let index = await this.nextIndex({ owner:ownerId, serviceId: ctx.params.serviceId });
                // prepare token
                let tok = await this.serializer.serialize(ctx.params.token);  
                
                let query = "INSERT INTO " + this.contextTable + " (owner,service,idx,value,oek,iv,tok) VALUES (:owner,:service,:idx,:value,:oek,:iv,:tok);";
                let params = { 
                    owner: ownerId, 
                    service: ctx.params.serviceId, 
                    idx: `${index}`,
                    value,
                    oek: oek.id,
                    iv: iv.toString("hex"),
                    tok
                };
                try {
                    await this.cassandra.execute(query, params, {prepare: true});
                    return true;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra insert error", { error: err.message, query: query, params: params });
                    return false;
                }
            }
        },
        
        /**
         * Fetch next entry from the queue 
         * 
         * @param {String} serviceId        - uuid
         * @param {String} workerId         - external unique id of the worker, can be either uid or a string
         * @param {Number} timeToRecover    - time in ms after unacknowledged entries are reassigned
         * 
         * @returns {Object} value
         */
        fetch: {
            acl: "before",
            params: {
                serviceId: { type: "uuid", optional: true },
                workerId: { type: "string" },
                timeToRecover: { type: "number", optional: true }
            },
            async handler(ctx) {
                let serviceId = ctx?.meta?.service?.serviceId ?? (ctx?.params?.serviceId ?? null );
                let ownerId = ctx?.meta?.ownerId ?? null;
                if (!ownerId || !serviceId) throw new Error("not authenticated");

                // get next index
                let timeToRecover = ctx.params.timeToRecover || 1000 * 60;  // default: 60 s
                let index = await this.fetchNext({ owner: ownerId, serviceId, workerId: ctx.params.workerId, timeToRecover });
                
                if (!index || index === 0) return null;
                
                let query = "SELECT owner, service, idx, value, oek, iv FROM " + this.contextTable;
                query += " WHERE owner = :owner AND service = :service AND idx = :idx;";
                let params = { 
                    owner: ownerId, 
                    service: serviceId, 
                    idx: index
                };
                try {
                    let result = await this.cassandra.execute(query, params, { prepare: true });
                    let row = result.first();
                    if (row) {

                        let oekId = row.get("oek");
                        let iv = Buffer.from(row.get("iv"), "hex");
                        let encrypted = row.get("value");
                        let value = null;
                        
                        // get owner's encryption key
                        let oek;
                        try {
                            oek = await this.getKey({ ctx: ctx, id: oekId });
                        } catch (err) {
                            this.logger.Error("Failed to retrieve owner encryption key", { ownerId, key: oekId });
                            throw new Error("failed to retrieve owner encryption key");
                        }

                        // decrypt value
                        try {
                            // hash received key with salt
                            let key = crypto.pbkdf2Sync(oek.key, iv, this.encryption.iterations, this.encryption.keylen, this.encryption.digest);
                            value = this.decrypt({ encrypted: encrypted, secret: key, iv: iv });
                        } catch (err) {
                            throw new Error("failed to decrypt");
                        }
                        
                        // deserialize value
                        value = await this.serializer.deserialize(value);
                        return value;
                    } else {
                        this.logger.debug("Unvalid or empty result", { result, first: row, query, params });
                        return null;
                    }
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra select error", { error: err.message, query });
                    return {};
                }
            }
        },

        /**
         * Confirm the last fetched task 
         * 
         * @param {String} serviceId        - uuid
         * @param {String} workerId         - external unique id of the worker, can be either uid or a string
         * @param {Any}    result           - optional: result to store in the context
         * @param {Object} error            - optional: error object, if handling failed
         * 
         * @returns {Boolean} result
         */
        ack: {
            acl: "before",
            params: {
                serviceId: { type: "uuid", optional: true },
                workerId: { type: "string" },
                result: { type: "any", optional: true },
                error: { type: "object", optional: true }
            },
            async handler(ctx) {
                let serviceId = ctx?.meta?.service?.serviceId ?? (ctx?.params?.serviceId ?? null );
                let ownerId = ctx?.meta?.ownerId ?? null;
                if (!ownerId || !serviceId) throw new Error("not authenticated");

                let index = await this.getCurrent({ owner: ownerId, serviceId, workerId: ctx.params.workerId });
                if (!index || index === 0) return false;
                
                // get token
                let token;
                let query = "SELECT tok FROM " + this.contextTable;
                query += " WHERE owner = :owner AND service = :service AND idx = :idx;";
                let params = { 
                    owner: ownerId, 
                    service: ctx.params.serviceId, 
                    idx: index
                };
                try {
                    let result = await this.cassandra.execute(query, params, { prepare: true });
                    let row = result.first();
                    if (row) {
                        // deserialize token
                        token = await this.serializer.deserialize(row.get("tok"));
                    }
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra select error", { error: err.message, query });
                    return false;
                }
                
                try {
                    let result = await this.ack({ owner: ownerId, serviceId, workerId: ctx.params.workerId });
                    // given token ?
                    if (token && token.processId) {
                        let params = {
                            token
                        };
                        if (ctx.params.result) params.result = ctx.params.result;
                        if (ctx.params.error) params.error = ctx.params.error;
                        this.logger.debug("call activity.completed", { params, meta: ctx.meta });
                        await ctx.call(this.services.activity + ".completed", params, { meta: ctx.meta });
                    }
                    
                    if (result && result >= 0 ) return true;
                    return false;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Redis delete error", { error: err.message, params: ctx.params });
                    return false;
                }
            }
        },
        
        /**
         * Set start index to read from
         * 
         * @param {String} serviceId     - uuid
         * @param {Number} last          - index 
         * 
         * @returns {Boolean} result
         */
        rewind: {
            acl: "before",
            params: {
                serviceId: { type: "uuid", optional: true },
                last: { type: "number" }
            },
            async handler(ctx) {
                let serviceId = ctx?.meta?.service?.serviceId ?? (ctx?.params?.serviceId ?? null );
                let ownerId = ctx?.meta?.ownerId ?? null;
                if (!ownerId || !serviceId) throw new Error("not authenticated");

                try {
                    let result = await this.rewind({ owner: ownerId, serviceId, last: `${ctx.params.last}` });
                    return result;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Redis rewind error", { error: err.message, params: ctx.params, serviceId });
                    return false;
                }
            }
        },
        
        /**
         * Get queue info from bookkeeper 
         * 
         * @param {String} serviceId        - uuid
         * 
         * @returns {Object} info           - { INDEX, FETCHED, unconfirmed tasks }
         */
        info: {
            acl: "before",
            params: {
                serviceId: { type: "uuid", optional: true }
            },
            async handler(ctx) {
                let serviceId = ctx?.meta?.service?.serviceId ?? (ctx?.params?.serviceId ?? null );
                let ownerId = ctx?.meta?.ownerId ?? null;
                if (!ownerId || !serviceId) throw new Error("not authenticated");
                
                try {
                    let result = await this.info({ owner: ownerId, serviceId });
                    return result;
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Redis request info error", { error: err.message, params: ctx.params, serviceId });
                    return null;
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
        
        async getKey ({ ctx = null, id = null } = {}) {
            
            let result = {};
            
            // try to retrieve from keys service
            let opts;
            if ( ctx ) opts = { meta: ctx.meta };
            let params = { 
                service: this.name
            };
            if ( id ) params.id = id;
            
            // call key service and retrieve keys
            try {
                result = await this.broker.call(this.services.keys + ".getOek", params, opts);
                this.logger.debug("Got key from key service", { id: id });
            } catch (err) {
                this.logger.error("Failed to receive key from key service", { id: id, meta: ctx.meta });
                throw err;
            }
            if (!result.id || !result.key) throw new Error("Failed to receive key from service", { result: result });
            return result;
        },
        
        encrypt ({ value = ".", secret, iv }) {
            let cipher = crypto.createCipheriv("aes-256-cbc", secret, iv);
            let encrypted = cipher.update(value, "utf8", "hex");
            encrypted += cipher.final("hex");
            return encrypted;
        },

        decrypt ({ encrypted, secret, iv }) {
            let decipher = crypto.createDecipheriv("aes-256-cbc", secret, iv);
            let decrypted = decipher.update(encrypted, "hex", "utf8");
            decrypted += decipher.final("utf8");
            return decrypted;            
        },
        
        async connect () {

            // connect to cassandra cluster
            await this.cassandra.connect();
            this.logger.info("Connected to cassandra", { contactPoints: this.contactPoints, datacenter: this.datacenter, keyspace: this.keyspace });
            
            // validate parameters
            // TODO! pattern doesn't work...
            let params = {
                keyspace: this.keyspace, 
                tablename: this.contextTable
            };
            let schema = {
                keyspace: { type: "string", trim: true },
                //tablename: { type: "string", trim: true, pattern: "[a-z][a-z0-9]*(_[a-z0-9]+)*", patternFlags: "g" } // doesn't work
                //tablename: { type: "string", trim: true, pattern: /[a-z][a-z0-9]*(_[a-z0-9]+)*/ } // doesn't work
                tablename: { type: "string", trim: true }
            };
            /*
            let valid = await this.broker.validator.validate(params,schema);
            if (!valid) {
                this.logger.error("Validation error", { params: params, schema: schema });
                throw new Error("Unalid table parameters. Cannot init cassandra database.");
            }
            */
            
            // create tables, if not exists
            let query = `CREATE TABLE IF NOT EXISTS ${this.keyspace}.${this.contextTable} `;
            query += " ( owner varchar, service uuid, idx varchar, value varchar, oek uuid, iv varchar, tok varchar, PRIMARY KEY (owner,service,idx) ) ";
            query += " WITH comment = 'storing worker queues';";
            await this.cassandra.execute(query);

        },
        
        async disconnect () {

            // close all open connections to cassandra
            await this.cassandra.shutdown();
            this.logger.info("Disconnected from cassandra", { contactPoints: this.contactPoints, datacenter: this.datacenter, keyspace: this.keyspace });
            
        }
        
    },

    /**
     * Service created lifecycle event handler
     */
    async created() {

        // encryption setup
        this.encryption = {
            iterations: 1000,
            ivlen: 16,
            keylen: 32,
            digest: "sha512"
        };
        
        this.serializer = new Serializer();

        // cassandra setup
        this.contactPoints = (this.settings?.cassandra?.contactPoints ?? "127.0.0.1" ).split(",");
        this.datacenter = this.settings?.cassandra?.datacenter ?? "datacenter1";
        this.keyspace = this.settings?.cassandra?.keyspace ?? "imicros_flow";
        this.contextTable = this.settings?.cassandra?.queueTable ?? "queue";
        this.cassandra = new Cassandra.Client({ contactPoints: this.contactPoints, localDataCenter: this.datacenter, keyspace: this.keyspace });

        // set actions
        this.services = {
            keys: this.settings?.services?.keys ?? "keys",
            activity: this.settings?.services?.activity ?? "activity"
        };        
        
        this.broker.waitForServices(Object.values(this.services));
        
    },

    /**
     * Service started lifecycle event handler
     */
    async started() {

        // connect to db
        await this.connect();
        
    },

    /**
     * Service stopped lifecycle event handler
     */
    async stopped() {
        
        // disconnect from db
        await this.disconnect();
        
    }

};