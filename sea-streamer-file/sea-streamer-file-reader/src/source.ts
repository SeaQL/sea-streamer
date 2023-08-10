import { NotEnoughBytes } from "./error";

export interface ByteSource {
    async requestBytes(size: bigint): Promise<Buffer | NotEnoughBytes>;
}