import { DynFileSource } from "./dyn_file";
import { FileErr } from "./error";
import { Header } from "./format";
import { ByteSource } from "./source";
import { StreamMode } from "./types";

export class MessageSource implements ByteSource {
    private header: Header;
    private source: DynFileSource;
    private buffer: Buffer;
    private offset: bigint;
    // private beacon: [number, Marker[]];
    private pending: Message | null;

    new(path: string, mode: StreamMode) {
        switch (mode) {
            case StreamMode.Live:
            case StreamMode.LiveReplay:
                throw new Error("Not implemented yet");
                break;
            case StreamMode.Replay:
                this.source = new FileReader(path);
                break;
        }
    }

    async open() {
        await this.source.open();
    }

    beaconInterval(): bigint {
        this.header.beaconInterval
    }

    hasBeacon(offset: bigint): Option<number | null> {
        if offset > 0 && offset % this.beaconInterval() == 0 {
            return Number(offset / this.beaconInterval());
        } else {
            return null;
        }
    }

    async requestBytes(size: usize): Promise<Bytes | FileErr> {
        // loop {
        //     if let Some(i) = this.hasBeacon(this.offset) {
        //         let beacon = Beacon::read_from(&mut this.source).await?;
        //         this.offset += beacon.size() as bigint;
        //         this.beacon = (i, beacon.items);
        //     }

        //     let chunk = std::cmp::min(
        //         size - this.buffer.size(), // remaining size
        //         this.beaconInterval() as usize - (this.offset % this.beaconInterval()) as usize, // should not read past the next beacon
        //     );
        //     let bytes = this.source.requestBytes(chunk).await?;
        //     this.offset += chunk as bigint;
        //     this.buffer.append(bytes); // these are message bytes

        //     debug_assert!(this.buffer.size() <= size, "we should never over-read");
        //     if this.buffer.size() == size {
        //         return Ok(this.buffer.consume(size));
        //     }
        // }
    }
}