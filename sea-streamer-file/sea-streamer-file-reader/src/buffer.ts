import { Buffer as SystemBuffer } from 'node:buffer';
import { FileErr, FileErrType } from './error';
import { ByteSource } from './source';

export class Buffer implements ByteSource {
    buffer: SystemBuffer;

    constructor() {
        this.buffer = SystemBuffer.alloc(0);
    }

    size(): bigint {
        return BigInt(this.buffer.length);
    }

    byteAt(at: number): number {
        return this.buffer.readUInt8(at);
    }

    consume(size: bigint): Buffer {
        const bytes = SystemBuffer.from(this.buffer.subarray(0, Number(size)));
        this.buffer = this.buffer.subarray(Number(size));
        const result = new Buffer();
        result.buffer = bytes;
        return result;
    }

    append(bytes: SystemBuffer | Buffer) {
        if (bytes instanceof SystemBuffer) {
            this.buffer = SystemBuffer.concat([this.buffer, bytes]);
        } else if (bytes instanceof Buffer) {
            this.buffer = SystemBuffer.concat([this.buffer, bytes.buffer]);
        }
    }

    toString(): string {
        return this.buffer.toString("utf8");
    }

    clear() {
        this.buffer = SystemBuffer.alloc(0);
    }

    async requestBytes(size: bigint): Promise<Buffer | FileErr> {
        if (size <= this.size()) {
            return this.consume(size);
        } else {
            return new FileErr(FileErrType.NotEnoughBytes);
        }
    }
}