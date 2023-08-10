import { Buffer } from 'node:buffer';
import { Timestamp, StreamKey, SeqNo, ShardId } from "./types";

export interface HeaderV1 {
    fileName: string;
    createdAt: Timestamp;
    beaconInterval: bigint;
}

export type Header = HeaderV1;
export const HEADER_SIZE: bigint = 128n;

export interface MessageHeader {
    streamKey: StreamKey;
    shardId: ShardId;
    sequence: SeqNo;
    timestamp: Timestamp;
}

export interface MessageFrame {
    message: Message;
    checksum: bigint;
}

export interface Message {
    header: MessageHeader;
    payload: Buffer;
}