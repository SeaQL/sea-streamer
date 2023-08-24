import { Buffer } from "./buffer";
import { SeqPosEnum, SeqPos } from "./types";
import { FileErr, FileErrType } from "./error";
import { DynFileSource, FileSourceType } from "./dyn_file";
import { AsyncFile, FileReader } from "./file";
import { performance } from "node:perf_hooks";

export interface ByteSource {
    requestBytes(size: bigint): Promise<Buffer | FileErr>;
}

/**
 * `FileSource` treats files as a live stream of bytes.
 * It will read til the end, and will resume reading when the file grows.
 * The async API allows you to request how many bytes you need, and it will wait for those
 * bytes to come in a non-blocking fashion.
 */
export class FileSource implements ByteSource, DynFileSource {
    private file: AsyncFile;
    /// This is the user's read offset, not the same as file's read pos
    private offset: bigint;
    private buffer: Buffer;
    private timeout: number;

    /** @internal */
    constructor(file: AsyncFile, offset: bigint, buffer: Buffer) {
        this.file = file;
        this.offset = offset;
        this.buffer = buffer;
        this.timeout = 10 * 1000;
    }

    static async new(path: string): Promise<FileSource> {
        const file = await AsyncFile.openRead(path);
        const reader = new FileSource(file, 0n, new Buffer());
        return reader;
    }

    async seek(to: SeqPosEnum): Promise<bigint | FileErr> {
        let result = await this.file.seek(to);
        if (result instanceof Error) {
            result = await this.file.seek(new SeqPos.End());
            if (result instanceof Error) {
                throwNewError("Should be unreachable");
            }
        }
        this.offset = result;
        this.buffer = new Buffer();
        return this.offset;
    }

    switchTo(type: FileSourceType): DynFileSource {
        if (type == FileSourceType.FileReader) {
            return new FileReader(this.file, this.offset, this.buffer);
        } else {
            return this;
        }
    }

    getOffset(): bigint {
        return this.offset;
    }

    fileSize(): bigint {
        return this.file.getSize();
    }

    async resize() {
        await this.file.resize();
    }

    sourceType(): FileSourceType {
        return FileSourceType.FileSource;
    }

    /**
     * Stream N bytes from file. If there is not enough bytes, it will wait until there are, like `tail -f`.
     * If there are enough bytes in the buffer, it yields immediately.
     */
    async requestBytes(size: bigint): Promise<Buffer | FileErr> {
        const startTime = performance.now();
        let wait = 0;
        while (true) {
            if (this.buffer.size() >= size) {
                this.offset += size;
                return this.buffer.consume(size);
            }
            const bytes = await this.file.read();
            if (bytes.length) {
                this.buffer.append(bytes);
            } else {
                const now = performance.now();
                if (now - startTime > this.timeout) {
                    return new FileErr(FileErrType.TimedOut);
                }
                await sleep(wait);
                wait = Math.min(Math.max(1, wait * 2), 1024);
            }
        }
    }

    setTimeout(ms: number) {
        this.timeout = ms;
    }
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function throwNewError(errMsg: string): never {
    throw new Error(errMsg);
}