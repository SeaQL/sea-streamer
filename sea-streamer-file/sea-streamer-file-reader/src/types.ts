export const SEA_STREAMER_INTERNAL: string = "SEA_STREAMER_INTERNAL";
export const PULSE_MESSAGE: string = "PULSE";
export const END_OF_STREAM: string = "EOS";
export const EOS_MESSAGE_SIZE: bigint = 56n;

export type Timestamp = Date;
export class StreamKey {
    name: string;
    constructor(name: string) {
        this.name = name;
    }
}
export class SeqNo {
    no: bigint;
    constructor(no: bigint) {
        this.no = no;
    }
}
export class ShardId {
    id: bigint;
    constructor(id: bigint) {
        this.id = id;
    }
}

class SeqPosBeginning { }
class SeqPosEnd { }
class SeqPosAt {
    at: bigint;
    constructor(at: bigint) {
        this.at = at;
    }
}
const SeqPos = {
    Beginning: SeqPosBeginning,
    End: SeqPosEnd,
    At: SeqPosAt,
}
export { SeqPos }
export type SeqPosEnum = SeqPosBeginning | SeqPosEnd | SeqPosAt;

export enum StreamMode {
    /**
     * Streaming from a file at the end
     */
    Live = "Live",
    /**
     * Replaying a dead file
     */
    Replay = "Replay",
    /**
     * Replaying a live file, might catch up to live
     */
    LiveReplay = "LiveReplay",
}