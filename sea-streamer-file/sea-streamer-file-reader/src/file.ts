import { open as openFile, FileHandle } from 'node:fs/promises';
import { Buffer as SystemBuffer } from 'node:buffer';
import { Buffer } from './buffer';
import { SeqPosEnum, SeqPos } from './types';
import { FileErr, FileErrType } from './error';
import { ByteSource } from './source';
import { DynFileSource, FileSourceType } from './dyn_file';

const BUFFER_SIZE: number = 1024;

export class FileReader implements ByteSource, DynFileSource {
    private file: AsyncFile;
    /// This is the user's read offset, not the same as file's read pos
    private offset: bigint;
    private buffer: Buffer;

    constructor(path: string) {
        this.file = new AsyncFile(path);
        this.offset = 0n;
        this.buffer = new Buffer();
    }

    async open() {
        await this.file.openRead();
    }

    async seek(to: SeqPosEnum): Promise<bigint | FileErr> {
        const result = await this.file.seek(to);
        if (result instanceof Error) {
            return new FileErr(FileErrType.NotEnoughBytes);
        }
        this.offset = result;
        this.buffer = new Buffer();
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

    sourceType(): FileSourceType {
        return FileSourceType.FileReader;
    }

    async requestBytes(size: bigint): Promise<Buffer | FileErr> {
        if (this.offset + size > this.file.getSize()) {
            return new FileErr(FileErrType.NotEnoughBytes);
        }
        while (true) {
            if (this.buffer.size() >= size) {
                this.offset += size;
                return this.buffer.consume(size);
            }
            const bytes = await this.file.read();
            if (!bytes.length) {
                return new FileErr(FileErrType.NotEnoughBytes);
            }
            this.buffer.append(bytes);
        }
    }
}

export class AsyncFile {
    private path: string;
    private file: FileHandle | null;
    private size: bigint;
    private pos: bigint;
    private buf: SystemBuffer;

    constructor(path: string) {
        this.path = path;
        this.file = null;
        this.size = 0n;
        this.pos = 0n;
        this.buf = SystemBuffer.alloc(BUFFER_SIZE);
    }

    async openRead() {
        this.file = await openFile(this.path, 'r');
        await this.resize();
        this.pos = 0n;
        this.buf = SystemBuffer.alloc(BUFFER_SIZE);
    }

    /**
     * @returns the returned Buffer is shared. consume immediately! it will be overwritten soon.
     */
    async read(): Promise<SystemBuffer> {
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