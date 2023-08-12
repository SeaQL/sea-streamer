import { FileErr } from "./error";
import { SeqPosEnum } from "./types";

export enum FileSourceType {
    FileReader,
    FileSource,
}

export interface DynFileSource {
    sourceType(): FileSourceType;
    seek(to: SeqPosEnum): Promise<bigint | FileErr>;
    // switchTo(type: FileSourceType): Promise<DynFileSource>;
    getOffset(): bigint;
    getFileSize(): bigint;
}