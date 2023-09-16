import { FileErr } from "./error";
import { MessageSource, isEndOfStream } from "./message";
import { StreamMode } from "./types";
import { Message } from "./format";

export interface CtrlMsg {
    cmd: "open" | "more" | "exit";
    path?: string;
}

export interface IpcMessage {
    messages: Message[];
    status: StatusUpdate;
}

export interface StatusUpdate {
    fileSize: bigint;
    readFrom: bigint;
    readUpTo: bigint;
}

let sleepFor = 1;
let quota = 10000;

process.on("message", (msg) => onMessage(msg as CtrlMsg));

function onMessage(ctrl: CtrlMsg) {
    if (ctrl.cmd === "open") {
        open(ctrl.path!);
    } else if (ctrl.cmd === "more") {
        sleepFor = 1;
        quota = 10000;
    } else if (ctrl.cmd === "exit") {
        process.exit(0);
    } else {
        process.send!({ error: "Unknown cmd." }); process.exit(1);
    }
}

async function open(path: string) {
    if (process.send === undefined) {
        throw new Error("Can only be run in a subprocess");
    }

    let source;
    try {
        source = await MessageSource.new(path, StreamMode.LiveReplay);
        if (source instanceof FileErr) { process.send!({ error: "Failed to read file header" }); process.exit(1); }
    } catch (e) {
        process.send!({ error: `Failed to open file: ${e}` }); process.exit(1);
    }
    run(source);
}

async function run(source: MessageSource) {
    const batchSize = 100;
    const buffer = [];
    let ended = false;

    while (!ended) {
        if (quota === 0) {
            await sleep(sleepFor);
            if (sleepFor < 1024) {
                sleepFor <<= 1;
            }
        }
        for (let i = 0; i < batchSize; i++) {
            const message = await source.next();
            if (message instanceof FileErr) { process.send!({ error: message.toString() }); process.exit(1); }
            buffer.push(message);
            if (isEndOfStream(message)) {
                ended = true;
                break;
            }
        }
        process.send!({ messages: buffer, status: getStatus() });
        quota -= buffer.length;
        buffer.length = 0;
    }

    process.send!({ messages: buffer, status: getStatus() });
    await source.close();

    function getStatus(): StatusUpdate {
        return {
            fileSize: source.knownSize(),
            readFrom: source.getReadFrom(),
            readUpTo: source.getOffset(),
        };
    }
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}