import { Buffer } from "./buffer";
import { DynFileSource } from "./dyn_file";
import { FileErr, FileErrType } from "./error";
import { FileReader } from "./file";
import { Header, Beacon, Marker, Message, MessageFrame } from "./format";
import { ByteSource, FileSource } from "./source";
import { SEA_STREAMER_INTERNAL, EOS_MESSAGE_SIZE, SeqPos, SeqPosEnum, StreamMode } from "./types";

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
                file = await FileSource.new(path);
                break;
            case StreamMode.Replay:
                file = await FileReader.new(path);
                break;
        }
        const header = await Header.readFrom(file); if (header instanceof FileErr) { return header; }
        Header.size() <= header.beaconInterval || throwNewError("Header size must be smaller than beaconInterval");

        const source = new MessageSource(mode, header, file);
        if (mode === StreamMode.Live) {
            await source.rewind(new SeqPos.End());
        }
        return source;
    }

    async close(): Promise<void> {
        await this.source.close();
    }

    fileHeader(): Header {
        return this.header;
    }

    /**
     * Rewind the message stream to a coarse position.
     * SeqNo is regarded as the N-th beacon.
     * Returns the current location in terms of N-th beacon.
     */
    async rewind(target: SeqPosEnum): Promise<number | FileErr> {
        let pos;
        if (target instanceof SeqPos.Beginning) {
            pos = new SeqPos.At(Header.size());
        } else if (target instanceof SeqPos.End) {
            pos = target;
        } else if (target instanceof SeqPos.At) {
            if (target.at === 0n) {
                pos = Header.size();
            } else {
                let at = target.at * this.beaconInterval();
                if (at < this.knownSize()) {
                    pos = new SeqPos.At(at);
                } else {
                    pos = new SeqPos.End();
                }
            }
        } else {
            throwNewError("unreachable");
        }

        const offset = await this.source.seek(pos); if (offset instanceof FileErr) { return offset; }
        this.offset = offset;

        // Align at a beacon
        if (pos instanceof SeqPos.End) {
            let max = this.knownSize() - (this.knownSize() % this.beaconInterval());
            max = bigintMax(max, Header.size());
            let pos;
            if (target instanceof SeqPos.End) {
                pos = max;
            } else if (target instanceof SeqPos.At) {
                let at = target.at * this.beaconInterval();
                if (at < this.knownSize()) {
                    pos = at;
                } else {
                    pos = max;
                }
            } else {
                throwNewError("unreachable");
            }
            if (this.offset !== pos) {
                const offset = await this.source.seek(new SeqPos.At(pos)); if (offset instanceof FileErr) { return offset; }
                this.offset = offset;
            }
        }

        this.buffer.clear();
        this.clearBeacon();

        // Read until the start of the next message
        while (true) {
            const i = this.hasBeacon(this.offset); if (i === null) { break; }
            const beacon = await Beacon.readFrom(this.source); if (beacon instanceof FileErr) { return beacon; }
            const beaconSize = beacon.size();
            this.offset += beaconSize;
            this.beacon = [i, beacon.items];

            const bytes = await this.source.requestBytes(bigintMin(
                beacon.remainingMessagesBytes,
                this.beaconInterval() - beaconSize,
            ));
            if (bytes instanceof FileErr) { return bytes; }
            this.offset += bytes.size();
        }

        // Now we are at the first message after the last beacon,
        // we want to consume all messages up to known size
        if (target instanceof SeqPos.End && this.offset < this.knownSize()) {
            let next = this.offset;
            const buffer = await this.source.requestBytes(this.knownSize() - this.offset);
            if (buffer instanceof FileErr) { return buffer; }
            while (true) {
                const message = await MessageFrame.readFrom(buffer);
                if (message instanceof FileErr) { break; }
                next += message.size();
            }
            const offset = await this.source.seek(new SeqPos.At(next));
            if (offset instanceof FileErr) { return offset; }
            this.offset = offset;
        }

        return Number(this.offset / this.beaconInterval());
    }

    async isStreamEnded(): Promise<boolean | FileErr> {
        const ES = EOS_MESSAGE_SIZE;
        // read the last beacon
        const anchor = this.source.fileSize() - this.source.fileSize() % this.beaconInterval();
        let crossed;
        let beacon;
        if (anchor !== 0n) {
            const offset = await this.source.seek(new SeqPos.At(anchor)); if (offset instanceof FileErr) { return offset; }
            beacon = await Beacon.readFrom(this.source); if (beacon instanceof FileErr) { return beacon; }
            crossed = anchor + beacon.size() + ES > this.source.fileSize();
        } else {
            crossed = false;
        }

        let buffer;
        if (crossed) {
            // this is a special case where the last message crossed a beacon
            if (beacon === undefined) { throwNewError("unreachable"); }
            // read the remaining half
            const half = await this.source.requestBytes(beacon.remainingMessagesBytes); if (half instanceof FileErr) { return half; }
            // read the first half
            const at = anchor - (ES - beacon.remainingMessagesBytes);
            const offset = await this.source.seek(new SeqPos.At(at)); if (offset instanceof FileErr) { return offset; }
            buffer = await this.source.requestBytes(ES - beacon.remainingMessagesBytes); if (buffer instanceof FileErr) { return buffer; }
            // stitch them together
            buffer.append(half);
        } else {
            const at = this.source.fileSize() - ES;
            const offset = await this.source.seek(new SeqPos.At(at)); if (offset instanceof FileErr) { return offset; }
            buffer = await this.source.requestBytes(ES); if (buffer instanceof FileErr) { return buffer; }
        }

        // restore original state
        const result = await this.source.seek(new SeqPos.At(this.offset)); if (result instanceof FileErr) { return result; }
        // final check
        const message = await MessageFrame.readFrom(buffer);
        if (message instanceof FileErr) { return false; }
        return isEndOfStream(message.message);
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

    clearBeacon() {
        this.beacon[0] = 0;
        this.beacon[1].length = 0;
    }

    knownSize(): bigint {
        return this.source.fileSize();
    }

    setTimeout(ms: number) {
        this.source.setTimeout(ms);
    }
}

function bigintMin(a: bigint, b: bigint): bigint {
    return a < b ? a : b;
}

function bigintMax(a: bigint, b: bigint): bigint {
    return a > b ? a : b;
}

function throwNewError(errMsg: string): never {
    throw new Error(errMsg);
}

export function isEndOfStream(message: Message): boolean {
    return message.header.streamKey.name === SEA_STREAMER_INTERNAL
        && message.payload.toString() === END_OF_STREAM;
}