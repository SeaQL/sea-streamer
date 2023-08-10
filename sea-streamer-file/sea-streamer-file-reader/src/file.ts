import { open as openFile, FileHandle } from 'node:fs/promises';
import { Buffer } from 'node:buffer';
import { SeqPosEnum, SeqPos } from './types';
import { NotEnoughBytes } from './error';

const BUFFER_SIZE: number = 1024;

export class FileReader {
    private file: AsyncFile;
    private offset: bigint;
    private buffer: Buffer;

    constructor(path: string) {
        this.file = new AsyncFile(path);
        this.offset = 0n;
        this.buffer = Buffer.alloc(0);
    }

    async open() {
        await this.file.openRead();
    }

    async seek(to: SeqPosEnum): Promise<bigint | NotEnoughBytes> {
        const result = await this.file.seek(to);
        if (result instanceof Error) {
            return NotEnoughBytes;
        }
        this.offset = result;
        this.buffer = Buffer.alloc(0);
        return this.offset;
    }

    getOffset(): bigint {
        return this.offset;
    }

    getFileSize(): bigint {
        return this.file.getSize();
    }

    async resize() {
        await this.file.resize();
    }

    async requestBytes(size: bigint): Promise<Buffer | NotEnoughBytes> {
        if (this.offset + size > this.file.getSize()) {
            return NotEnoughBytes;
        }
        while (true) {
            if (this.buffer.length >= size) {
                const bytes = Buffer.from(this.buffer.subarray(0, Number(size)));
                this.buffer = this.buffer.subarray(Number(size));
                return bytes;
            }
            const bytes = await this.file.read();
            if (!bytes.length) {
                return NotEnoughBytes;
            }
            this.buffer = Buffer.concat([this.buffer, bytes]);
        }
    }
}

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

    async openRead() {
        this.file = await openFile(this.path, 'r');
        await this.resize();
        this.pos = 0n;
        this.buf = Buffer.alloc(BUFFER_SIZE);
    }

    /**
     * @returns the returned Buffer is shared. consume immediately! it will be overwritten soon.
     */
    async read(): Promise<Buffer> {
        const file = this.file ?? throwNewError("File not yet opened");
        const bytesRead = (await file.read(this.buf, 0, BUFFER_SIZE, Number(this.pos))).bytesRead;
        this.pos += BigInt(bytesRead);
        this.size = this.pos > this.size ? this.pos : this.size;
        return this.buf.subarray(0, bytesRead);
    }

    async seek(to: SeqPosEnum): Promise<bigint | Error> {
        let offset;
        if (to instanceof SeqPos.Beginning) {
            offset = 0n;
        } else if (to instanceof SeqPos.End) {
            await this.resize();
            offset = this.size;
        } else if (to instanceof SeqPos.At) {
            await this.resize();
            offset = to.at;
        } else {
            throwNewError("unreachable");
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