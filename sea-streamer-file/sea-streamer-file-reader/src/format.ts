import { Buffer } from './buffer';
import { Timestamp, StreamKey, SeqNo, ShardId } from "./types";
import { FileErr, FileErrType } from './error';
import { ByteSource } from './source';
import { crc16Cdma2000 } from './crc';

export class Header {
    fileName: string;
    createdAt: Timestamp;
    beaconInterval: bigint;

    constructor(
        fileName: string,
        createdAt: Timestamp,
        beaconInterval: bigint,
    ) {
        this.fileName = fileName;
        this.createdAt = createdAt;
        this.beaconInterval = beaconInterval;
    }

    toJson(): any {
        return {
            fileName: this.fileName,
            createdAt: this.createdAt,
            beaconInterval: Number(this.beaconInterval),
        }
    }

    static async readFrom(file: ByteSource): Promise<Header | FileErr> {
        const bytes = await Bytes.readFrom(file, 3n); if (bytes instanceof FileErr) { return bytes; }
        if (bytes.byteAt(0) != 0x53) {
            return new FileErr(FileErrType.FormatErr__ByteMark);
        }
        if (bytes.byteAt(1) != 0x73) {
            return new FileErr(FileErrType.FormatErr__ByteMark);
        }
        if (bytes.byteAt(2) != 0x01) {
            return new FileErr(FileErrType.FormatErr__Version);
        }
        const fileName = await ShortString.readFrom(file); if (fileName instanceof FileErr) { return fileName; }
        const createdAt = await UnixTimestamp.readFrom(file); if (createdAt instanceof FileErr) { return createdAt; }
        const beaconInterval = await U32.readFrom(file); if (beaconInterval instanceof FileErr) { return beaconInterval; }
        const ret = new Header(
            fileName,
            createdAt,
            BigInt(beaconInterval),
        );
        const padding = await Bytes.readFrom(file, ret.paddingSize()); if (padding instanceof FileErr) { return padding; }
        return ret;
    }

    static size(): bigint {
        return HEADER_SIZE;
    }

    paddingSize(): bigint {
        return HEADER_SIZE
            - 3n
            - new ShortString(this.fileName).size()
            - UnixTimestamp.size()
            - U32.size()
    }
}

export const HEADER_SIZE: bigint = 128n;

export class MessageFrame {
    message: Message;
    checksum: number;

    constructor(
        message: Message,
        checksum: number,
    ) {
        this.message = message;
        this.checksum = checksum;
    }

    static async readFrom(file: ByteSource): Promise<MessageFrame | FileErr> {
        const header = await MessageHeader.readFrom(file); if (header instanceof FileErr) { return header; }
        const size = await U32.readFrom(file); if (size instanceof FileErr) { return size; }
        const payload = await Bytes.readFrom(file, BigInt(size)); if (payload instanceof FileErr) { return payload; }
        const checksum = await U16.readFrom(file); if (checksum instanceof FileErr) { return checksum; }
        const message = new Message(header, payload);
        const bytes = await Bytes.readFrom(file, 1n); if (bytes instanceof FileErr) { return bytes; }
        return new MessageFrame(message, checksum);
    }

    size(): bigint {
        return this.message.header.size()
            + U32.size()
            + this.message.payload.size()
            + U16.size()
            + 1n;
    }

    computeChecksum(): number {
        return crc16Cdma2000(this.message.payload.buffer);
    }
}

export class Message {
    header: MessageHeader;
    payload: Buffer;

    constructor(
        header: MessageHeader,
        payload: Buffer,
    ) {
        this.header = header;
        this.payload = payload;
    }
}

export class Beacon {
    remainingMessagesBytes: bigint;
    items: Marker[];

    constructor(
        remainingMessagesBytes: bigint,
        items: Marker[],
    ) {
        this.remainingMessagesBytes = remainingMessagesBytes;
        this.items = items;
    }

    static async readFrom(file: ByteSource): Promise<Beacon | FileErr> {
        const bytes1 = await Bytes.readFrom(file, 1n); if (bytes1 instanceof FileErr) { return bytes1; }
        const remainingMessagesBytes = await U32.readFrom(file); if (remainingMessagesBytes instanceof FileErr) { return remainingMessagesBytes; }
        const items = [];
        const bytes2 = await Bytes.readFrom(file, 1n); if (bytes2 instanceof FileErr) { return bytes2; }
        const num = bytes2.byteAt(0);
        for (let i = 0; i < num; i++) {
            const marker = await Marker.readFrom(file); if (marker instanceof FileErr) { return marker; }
            items.push(marker);
        }
        const bytes3 = await Bytes.readFrom(file, 1n); if (bytes3 instanceof FileErr) { return bytes3; }
        return new Beacon(
            BigInt(remainingMessagesBytes),
            items,
        );
    }

    size(): bigint {
        let size = 1n + U32.size() + 1n;
        for (const item of this.items) {
            size += item.size();
        }
        return size + 1n;
    }
}

export class Marker {
    header: MessageHeader;
    runningChecksum: number;

    constructor(
        header: MessageHeader,
        runningChecksum: number,
    ) {
        this.header = header;
        this.runningChecksum = runningChecksum;
    }

    toJson(): any {
        return {
            header: this.header.toJson(),
            runningChecksum: this.runningChecksum,
        }
    }

    static async readFrom(file: ByteSource): Promise<Marker | FileErr> {
        const header = await MessageHeader.readFrom(file); if (header instanceof FileErr) { return header; }
        const runningChecksum = await U16.readFrom(file); if (runningChecksum instanceof FileErr) { return runningChecksum; }
        return new Marker(
            header,
            runningChecksum,
        );
    }

    size(): bigint {
        return this.header.size() + U16.size();
    }

    static maxSize(): bigint {
        return MessageHeader.maxSize() + 2n;
    }
}

export class MessageHeader {
    streamKey: StreamKey;
    shardId: ShardId;
    sequence: SeqNo;
    timestamp: Timestamp;

    constructor(
        streamKey: StreamKey,
        shardId: ShardId,
        sequence: SeqNo,
        timestamp: Timestamp,
    ) {
        this.streamKey = streamKey;
        this.shardId = shardId;
        this.sequence = sequence;
        this.timestamp = timestamp;
    }

    toJson(): any {
        return {
            streamKey: this.streamKey.name,
            shardId: Number(this.shardId.id),
            sequence: Number(this.sequence.no),
            timestamp: this.timestamp.toISOString(),
        }
    }

    static async readFrom(file: ByteSource): Promise<MessageHeader | FileErr> {
        const streamKey = await ShortString.readFrom(file); if (streamKey instanceof FileErr) { return streamKey; }
        const shardId = await U64.readFrom(file); if (shardId instanceof FileErr) { return shardId; }
        const sequence = await U64.readFrom(file); if (sequence instanceof FileErr) { return sequence; }
        const timestamp = await UnixTimestamp.readFrom(file); if (timestamp instanceof FileErr) { return timestamp; }
        return new MessageHeader(
            new StreamKey(streamKey),
            new ShardId(shardId),
            new SeqNo(sequence),
            timestamp,
        );
    }

    size(): bigint {
        return (new ShortString(this.streamKey.name).size())
            + U64.size()
            + U64.size()
            + UnixTimestamp.size();
    }

    static maxSize(): bigint {
        return ShortString.maxSize() + U64.size() + U64.size() + UnixTimestamp.size();
    }
}

export class ShortString {
    string: string;

    constructor(string: string) {
        if (string.length > 255) {
            throw new Error("String too long");
        }
        this.string = string;
    }

    static async readFrom(file: ByteSource): Promise<string | FileErr> {
        const bytes = await Bytes.readFrom(file, 1n); if (bytes instanceof FileErr) { return bytes; }
        const len = bytes.byteAt(0);
        const string = await Bytes.readFrom(file, BigInt(len)); if (string instanceof FileErr) { return string; }
        return string.toString();
    }

    size(): bigint {
        return 1n + BigInt(this.string.length);
    }

    static maxSize(): bigint {
        return 256n;
    }
}

export class UnixTimestamp {

    static async readFrom(file: ByteSource): Promise<Timestamp | FileErr> {
        const ts = await U64.readFrom(file); if (ts instanceof FileErr) { return ts; }
        return new Date(Number(ts));
    }

    static size(): bigint {
        return U64.size();
    }
}

export class U64 {
    static async readFrom(file: ByteSource): Promise<bigint | FileErr> {
        const bytes = await Bytes.readFrom(file, 8n); if (bytes instanceof FileErr) { return bytes; }
        return bytes.buffer.readBigUInt64BE();
    }

    static size(): bigint {
        return 8n;
    }
}

export class U32 {
    static async readFrom(file: ByteSource): Promise<number | FileErr> {
        const bytes = await Bytes.readFrom(file, 4n); if (bytes instanceof FileErr) { return bytes; }
        return bytes.buffer.readUInt32BE();
    }

    static size(): bigint {
        return 4n;
    }
}

export class U16 {
    static async readFrom(file: ByteSource): Promise<number | FileErr> {
        const bytes = await Bytes.readFrom(file, 2n); if (bytes instanceof FileErr) { return bytes; }
        return bytes.buffer.readUInt16BE();
    }

    static size(): bigint {
        return 2n;
    }
}

export class Bytes {
    static async readFrom(src: ByteSource, size: bigint): Promise<Buffer | FileErr> {
        return await src.requestBytes(size);
    }
}