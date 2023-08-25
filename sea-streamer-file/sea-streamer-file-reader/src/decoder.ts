import * as process from "node:process";
import { FileErr } from "./error";
import { MessageSource, isEndOfStream } from "./message";
import { SeqPos, StreamMode } from "./types";
import { isUtf8 } from "./is_utf8";

async function main() {
    let path;
    for (let i = 0; i < process.argv.length; i++) {
        if (process.argv[i] == "--file") {
            path = process.argv[i + 1];
        }
    }
    if (!path) {
        throw new Error("Please specify the file path with `--file`");
    }
    const source = await MessageSource.new(path, StreamMode.LiveReplay);
    if (source instanceof FileErr) { throw new Error("Failed to read file header"); }
    console.log("#", JSON.stringify(source.fileHeader().toJson()));

    let beacon = 0;
    while (true) {
        const message = await source.next();
        if (message instanceof FileErr) {
            console.error(message);
            return;
        }
        const h = message.header;
        console.log(
            `[${h.timestamp.toISOString()} | ${h.streamKey.name} | ${h.sequence.no} | ${h.shardId.id}]`,
            isUtf8(message.payload.buffer) ? message.payload.toString() : "<BINARY BLOB>"
        );

        if (source.getBeacon()[0] != beacon) {
            beacon = source.getBeacon()[0];
            console.log("#", JSON.stringify(source.getBeacon()[1].map((x) => x.toJson())));
        }

        if (isEndOfStream(message)) {
            break;
        }
    }
}

main();