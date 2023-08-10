import { open as openFile, FileHandle } from 'node:fs/promises';
import { Buffer } from 'node:buffer';
import { SeqPos, SeqPosWhere } from './types';

const BUFFER_SIZE: number = 1024;

export class AsyncFile {
    private path: string;
    private file: FileHandle | null;
    private size: bigint;
    private pos: bigint;
    private buf: Buffer;

    constructor(path: string) {
        this.path = path;
        this.file = null;
        this.size = 0n;
        this.pos = 0n;
        this.buf = Buffer.alloc(BUFFER_SIZE);
    }

    async open_read() {
        this.file = await openFile(this.path, 'r');
        await this.resize();
        this.pos = 0n;
        this.buf = Buffer.alloc(BUFFER_SIZE);
    }

    async read(): Promise<Buffer> {
        const file = this.file ?? throwNewError("File not yet opened");
        const bytesRead = (await file.read(this.buf, 0, BUFFER_SIZE, Number(this.pos))).bytesRead;
        this.pos += BigInt(bytesRead);
        this.size = this.pos > this.size ? this.pos : this.size;
        const bytes = this.buf.subarray(0, bytesRead);
        return Buffer.from(bytes);
    }

    async seek(to: SeqPos): Promise<bigint | Error> {
        let offset;
        switch (to.where) {
            case SeqPosWhere.Beginning:
                offset = 0n;
                break;
            case SeqPosWhere.End:
                await this.resize();
                offset = this.size;
                break;
            case SeqPosWhere.At:
                await this.resize();
                offset = to.at;
                break;
        }
        if (offset <= this.size) {
            this.pos = offset;
            return this.pos;
        } else {
            return new Error("AsyncFile: seek beyond file size");
        }
    }

    getSize(): bigint {
        return this.size;
    }

    getPos(): bigint {
        return this.pos;
    }

    async resize() {
        const file = this.file ?? throwNewError("File not yet opened");
        this.size = (await file.stat({ bigint: true })).size;
    }

    async close() {
        if (this.file !== null) {
            await this.file.close();
        }
    }
}

function throwNewError(errMsg: string): never {
    throw new Error(errMsg);
}