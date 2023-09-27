import { FileErr } from "./error";
import { MessageSource, isEndOfStream } from "./message";
import { PULSE_MESSAGE, SEA_STREAMER_INTERNAL, SeqNo, SeqPos, ShardId, StreamKey, StreamMode } from "./types";
import { FileHeader, Message, MessageHeader } from "./format";
import { Buffer } from "./buffer";
import { Buffer as SystemBuffer } from "node:buffer";

export const DEFAULT_QUOTA: number = 10000;

export interface CtrlMsg {
    cmd: "open" | "more" | "seek" | "exit";
    /** file path */
    path?: string;
    /** n-th beacon */
    nth?: number;
}

export type IpcMessage = MetaUpdate | MessageUpdate;

export interface MetaUpdate {
    header: FileHeader;
}

export interface MessageUpdate {
    messages: Message[];
    status: StatusUpdate;
}

export function isMetaUpdate(m: IpcMessage): m is MetaUpdate {
    return typeof (m as MetaUpdate).header !== "undefined";
}

export function isMessageUpdate(m: IpcMessage): m is MessageUpdate {
    return typeof (m as MessageUpdate).messages !== "undefined";
}

export interface StatusUpdate {
    fileSize: bigint;
    readFrom: bigint;
    readUpTo: bigint;
}

enum State {
    Init,
    Running,
    PreSeek,
    Seeking,
}

let sleepFor = 1;
let quota = DEFAULT_QUOTA;
let global: {
    error: boolean;
    state: State;
    source: MessageSource | undefined;
} = {
    error: false,
    state: State.Init,
    source: undefined,
};

process.on("message", (msg) => onMessage(msg as CtrlMsg));

const process_log = (msg: string) => process.send!({ log: msg });

function onMessage(ctrl: CtrlMsg) {
    if (ctrl.cmd === "open") {
        open(ctrl.path!).then(run);
    } else if (ctrl.cmd === "more") {
        sleepFor = 1;
        quota = DEFAULT_QUOTA;
    } else if (ctrl.cmd === "seek") {
        if (global.state === State.Running) {
            process_log(`seek ${ctrl.nth}`);
            seek(ctrl.nth!).then(run);
        } else {
            process.send!({ error: "Not seekable" }); global.error = true; return;
        }
    } else if (ctrl.cmd === "exit") {
        if (global.error) {
            process.exit(1);
        } else {
            process.exit(0);
        }
    } else {
        process.send!({ error: "Unknown cmd." }); global.error = true; return;
    }
}

async function open(path: string) {
    if (process.send === undefined) {
        throw new Error("Can only be run in a subprocess");
    }

    let source;
    try {
        source = await MessageSource.new(path, StreamMode.LiveReplay);
        if (source instanceof FileErr) { process.send!({ error: "Failed to read file header" }); global.error = true; return; }
        global.source = source;
        process.send!({ header: source.fileHeader().toJson() });
    } catch (e) {
        process.send!({ error: `Failed to open file: ${e}` }); global.error = true; return;
    }
}

async function run() {
    if (global.error) {
        return;
    }
    global.state = State.Running as State;
    const source = global.source!;
    const batchSize = 100;
    const buffer = [];
    let ended = false;

    while (!ended) {
        if (global.state as State === State.PreSeek) { global.state = State.Seeking as State; return; }
        if (quota <= 0) {
            await sleep(sleepFor);
            if (sleepFor < 1024) {
                sleepFor <<= 1;
            }
            continue;
        }
        for (let i = 0; i < batchSize; i++) {
            const message = await source.next();
            if (message instanceof FileErr) { process.send!({ error: message.toString() }); global.error = true; return; }
            buffer.push(message);
            if (isEndOfStream(message)) {
                ended = true;
                break;
            }
        }
        if (global.state as State === State.PreSeek) { global.state = State.Seeking as State; return; }
        process.send!({ messages: buffer, status: getStatus() });
        quota -= buffer.length;
        buffer.length = 0;
    }

    process.send!({ messages: buffer, status: getStatus() });
    await source.close();
}

async function seek(nth: number) {
    if (global.error) {
        return;
    }
    global.state = State.PreSeek as State;
    while (global.state === State.PreSeek) { await sleep(10); }
    if (global.state === State.Seeking) {
        const source = global.source!;
        await source.rewind(new SeqPos.At(BigInt(nth)));
        process_log("rewinded");
        const payload = new Buffer();
        payload.append(SystemBuffer.from(PULSE_MESSAGE));
        const pulse = new Message(new MessageHeader(
            new StreamKey(SEA_STREAMER_INTERNAL),
            new ShardId(0n),
            new SeqNo(0n),
            new Date(),
        ), payload);
        process.send!({ messages: [pulse], status: getStatus() });
        quota = DEFAULT_QUOTA;
    } else {
        process.send!({ error: "Not seeking?" }); global.error = true; return;
    }
}

function getStatus(): StatusUpdate {
    return {
        fileSize: global.source!.knownSize(),
        readFrom: global.source!.getReadFrom(),
        readUpTo: global.source!.getOffset(),
    };
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}