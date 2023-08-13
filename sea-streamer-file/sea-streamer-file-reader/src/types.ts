export const SEA_STREAMER_INTERNAL: string = "SEA_STREAMER_INTERNAL";

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
    Live,
    /**
     * Replaying a dead file
     */
    Replay,
    /**
     * Replaying a live file, might catch up to live
     */
    LiveReplay,
}