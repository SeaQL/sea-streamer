export type Timestamp = Date;
export type StreamKey = string;
export type SeqNo = bigint;
export type ShardId = bigint;

export enum SeqPosWhere {
    Beginning,
    End,
    At,
}

export interface SeqPos {
    where: SeqPosWhere;
    at: bigint;
}