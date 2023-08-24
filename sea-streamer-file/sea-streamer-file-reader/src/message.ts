import { Buffer } from './buffer';
import { DynFileSource } from "./dyn_file";
import { FileErr, FileErrType } from "./error";
import { FileReader } from "./file";
import { Header, Beacon, Marker, Message, MessageFrame } from "./format";
import { ByteSource } from "./source";
import { SEA_STREAMER_INTERNAL, SeqPos, SeqPosEnum, StreamMode } from "./types";

export const END_OF_STREAM: string = "EOS";

export class MessageSource implements ByteSource {
    private mode: StreamMode;
    private header: Header;
    private source: DynFileSource & ByteSource;
    private buffer: Buffer;
    private offset: bigint;
    private beacon: [number, Marker[]];
    private pending: Message | null;

    private constructor(
        mode: StreamMode,
        header: Header,
        source: DynFileSource & ByteSource,
    ) {
        this.mode = mode;
        this.header = header;
        this.source = source;
        this.buffer = new Buffer();
        this.offset = Header.size();
        this.beacon = [0, []];
        this.pending = null;
    }

    static async new(path: string, mode: StreamMode): Promise<MessageSource | FileErr> {
        let file;
        switch (mode) {
            case StreamMode.Live:
            case StreamMode.LiveReplay:
                throw new Error("Not implemented yet");
                break;
            case StreamMode.Replay:
                file = await FileReader.new(path);
                break;
        }
        const header = await Header.readFrom(file); if (header instanceof FileErr) { return header; }
        Header.size() <= header.beaconInterval || throwNewError("Header size must be smaller than beaconInterval");

        const source = new MessageSource(mode, header, file);
        // if (mode === StreamMode.Live) {
        //     await source.rewind(SeqPos.End);
        // }
        return source;
    }

    fileHeader(): Header {
        return this.header;
    }

    beaconInterval(): bigint {
        return this.header.beaconInterval;
    }

    hasBeacon(offset: bigint): number | null {
        if (offset > 0 && offset % this.beaconInterval() === 0n) {
            return Number(offset / this.beaconInterval());
        } else {
            return null;
        }
    }

    getBeacon(): [number, Marker[]] {
        return this.beacon;
    }

    async rewind(pos: SeqPosEnum): Promise<void> {
        throwNewError("Unimplemented");
    }

    async requestBytes(size: bigint): Promise<Buffer | FileErr> {
        while (true) {
            const i = this.hasBeacon(this.offset);
            if (i !== null) {
                const beacon = await Beacon.readFrom(this.source); if (beacon instanceof FileErr) { return beacon; }
                this.offset += beacon.size();
                this.beacon = [i, beacon.items];
            }

            const chunk = bigintMin(
                size - this.buffer.size(), // remaining size
                this.beaconInterval() - (this.offset % this.beaconInterval()), // should not read past the next beacon
            );
            const bytes = await this.source.requestBytes(chunk); if (bytes instanceof FileErr) { return bytes; }
            this.offset += chunk;
            this.buffer.append(bytes); // these are message bytes

            this.buffer.size() <= size || throwNewError("we should never over-read");
            if (this.buffer.size() === size) {
                return this.buffer.consume(size);
            }
        }
    }

    async next(): Promise<Message | FileErr> {
        let message;
        if (this.pending !== null) {
            message = this.pending;
            this.pending = null;
            return message;
        } else {
            message = await MessageFrame.readFrom(this); if (message instanceof FileErr) { return message; }
        }
        let computed = message.computeChecksum();
        if (message.checksum !== computed) {
            return new FileErr(FileErrType.FormatErr__ChecksumErr, {
                received: message.checksum,
                computed,
            });
        } else {
            return message.message;
        }
    }
}

function bigintMin(a: bigint, b: bigint): bigint {
    return a < b ? a : b;
}

function throwNewError(errMsg: string): never {
    throw new Error(errMsg);
}

export function isEndOfStream(message: Message): boolean {
    return message.header.streamKey.name === SEA_STREAMER_INTERNAL
        && message.payload.toString() === END_OF_STREAM;
}