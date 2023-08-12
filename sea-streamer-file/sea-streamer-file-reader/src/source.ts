import { FileErr } from "./error";

export interface ByteSource {
    async requestBytes(size: bigint): Promise<Buffer | FileErr>;
}