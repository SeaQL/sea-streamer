import { FileErr } from "./error";
import { Buffer } from './buffer';

export interface ByteSource {
    requestBytes(size: bigint): Promise<Buffer | FileErr>;
}