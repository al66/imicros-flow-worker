
const n = 3000000;
const p = 100000;
const maxBatchSize = 1000;
let count = 0;
let commit = [];

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function sendBatch(last) {
    if (!commit.length) return;
    let current = commit[commit.length - 1].timestamp;
    if (current > last) return;

    let batch = [];
    let pick = commit.length > maxBatchSize ? maxBatchSize : commit.length;
    for (let i = 0; i < pick; i++) batch.push(commit.shift());
    // console.log({ sent: pick, current: current, last: last });
    await sleep(500);
    batch.forEach(e => e.resolve(e.index));
}

async function send(index) {
    count++;
    let received = Date.now();
    let p = new Promise(resolve => commit.push({ timestamp: received, msg: index, resolve: resolve}));
    
    if (commit.length >= maxBatchSize) {
        sendBatch();
    } else {
        setTimeout(function () { sendBatch(received); }, 50);
    }
    
    return p;
}


async function run() {
    let ts = Date.now();
    // for (let i = 0; i < n; i++) await send();
    for (let i = 0; i < n; i += p) {
        let ts = Date.now();
        let jobs = Array.from(Array(p),(x,index) => index + 1);
        await Promise.all(jobs.map(async (j) => await send(j)));
        let te = Date.now();
        console.log({
            "sent": p,
            "time (ms)": te-ts
        });
    }
    let te = Date.now();
    console.log({
        "total sent": count,
        "time (ms)": te-ts
    });
}

run();
