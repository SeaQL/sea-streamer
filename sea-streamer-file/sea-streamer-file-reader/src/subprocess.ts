import { FileErr } from "./error";
import { MessageSource, isEndOfStream } from "./message";
import { PULSE_MESSAGE, SEA_STREAMER_INTERNAL, SeqNo, SeqPos, ShardId, StreamKey, StreamMode } from "./types";
import { FileHeader, Message, MessageHeader } from "./format";
import { Buffer } from "./buffer";
import { Buffer as SystemBuffer } from "node:buffer";

export const DEFAULT_QUOTA: number = 10000;

export type CtrlMsg = OpenCmd | RunCmd | MoreCmd | SeekCmd | ExitCmd;

export interface OpenCmd {
    cmd: "open";
    /** file path */
    path: string;
}

export interface RunCmd {
    cmd: "run";
}

export interface MoreCmd {
    cmd: "more";
}

export interface SeekCmd {
    cmd: "seek";
    /** n-th beacon */
    nth: number;
}

export interface ExitCmd {
    cmd: "exit";
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
    Ready,
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
        open(ctrl.path);
    } else if (ctrl.cmd === "run") {
        run();
    } else if (ctrl.cmd === "more") {
        sleepFor = 1;
        quota = DEFAULT_QUOTA;
    } else if (ctrl.cmd === "seek") {
        process_log(`seek ${ctrl.nth}`);
        if (global.state === State.Init) {
            untilInit().then(() => seek(ctrl.nth)).then(run);
        } else if (global.state === State.Ready) {
            seek(ctrl.nth).then(run);
        } else if (global.state === State.Running) {
            preseek().then(() => seek(ctrl.nth)).then(run);
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
        global.state = State.Ready as State;
    } catch (e) {
        process.send!({ error: `Failed to open file: ${e}` }); global.error = true; return;
    }
}

async function run() {
    if (global.error) {
        return;
    }
    if (global.state === State.Init) {
        await untilInit();
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

async function untilInit() {
    if (global.error) {
        return;
    }
    if (global.state !== State.Init) {
        process.send!({ error: "Not Init?" }); global.error = true; return;
    }
    while (global.state === State.Init) { await sleep(1); }
}

async function preseek() {
    if (global.error) {
        return;
    }
    if (global.state !== State.Running) {
        process.send!({ error: "Not Running?" }); global.error = true; return;
    }
    global.state = State.PreSeek as State;
    while (global.state === State.PreSeek) { await sleep(10); }
    if (global.state !== State.Seeking) {
        process.send!({ error: "Not Seeking?" }); global.error = true; return;
    }
}

async function seek(nth: number) {
    if (global.error) {
        return;
    }
    global.state = State.Seeking as State;
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