// mock service activity

const activity = [];

const Activity = {
    name: "activity",

    actions: {
        completed: {
            params: {
                token: { 
                    type: "object",
                    props: {
                        processId: { type: "uuid" },
                        instanceId: { type: "uuid" },
                        elementId: { type: "uuid", optional: true },
                        type: { type: "string" },
                        status: { type: "string" },
                        user: { type: "object" },
                        ownerId: { type: "string" },
                        attributes: { type: "object", optional: true}
                    }
                },
                result: { type: "any", optional: true },
                error: { type: "object", optional: true }
            },
            async handler(ctx) {
                this.logger.info("activity.completed", { params: ctx.params, meta: ctx.meta });
                activity.push({ action: "activity.completed", params: ctx.params, meta: ctx.meta });
                return true;
            }
        }
    }
};

module.exports = {
    Activity,
    activity
};