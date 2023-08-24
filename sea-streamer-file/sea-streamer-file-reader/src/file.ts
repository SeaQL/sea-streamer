import { open as openFile, FileHandle } from 'node:fs/promises';
import { Buffer as SystemBuffer } from 'node:buffer';
import { Buffer } from './buffer';
import { SeqPosEnum, SeqPos } from './types';
import { FileErr, FileErrType } from './error';
import { ByteSource } from './source';
import { DynFileSource, FileSourceType } from './dyn_file';

const BUFFER_SIZE: number = 1024;

/**
 * A simple buffered and bounded file reader.
 * The implementation is much simpler than `FileSource`.
 *
 * `FileReader` treats file as a fixed depot of bytes.
 * Attempt to read beyond the end will result in a `NotEnoughBytes` error.
 */
export class FileReader implements ByteSource, DynFileSource {
    private file: AsyncFile;
    /// This is the user's read offset, not the same as file's read pos
    private offset: bigint;
    private buffer: Buffer;

    private constructor(file: AsyncFile) {
        this.file = file;
        this.offset = 0n;
        this.buffer = new Buffer();
    }

    static async new(path: string): Promise<FileReader> {
        const file = await AsyncFile.openRead(path);
        const reader = new FileReader(file);
        return reader;
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

/**
 * A minimal wrapper over system's File IO
 */
export class AsyncFile {
    private path: string;
    private file: FileHandle;
    private size: bigint;
    private pos: bigint;
    private buf: SystemBuffer;

    private constructor(path: string, file: FileHandle) {
        this.path = path;
        this.file = file;
        this.size = 0n;
        this.pos = 0n;
        this.buf = SystemBuffer.alloc(BUFFER_SIZE);
    }

    static async openRead(path: string): Promise<AsyncFile> {
        const handle = await openFile(path, 'r');
        const file = new AsyncFile(path, handle);
        await file.resize();
        return file;
    }

    /**
     * @returns the returned Buffer is shared. consume immediately! it will be overwritten soon.
     */
    async read(): Promise<SystemBuffer> {
        const bytesRead = (await this.file.read(this.buf, 0, BUFFER_SIZE, Number(this.pos))).bytesRead;
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
        this.size = (await this.file.stat({ bigint: true })).size;
    }

    async close() {
        await this.file.close();
    }
}

function throwNewError(errMsg: string): never {
    throw new Error(errMsg);
}