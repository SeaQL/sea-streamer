import { FileErr } from "./error";
import { SeqPosEnum } from "./types";

export enum FileSourceType {
    FileReader = "FileReader",
    FileSource = "FileSource",
}

export interface DynFileSource {
    sourceType(): FileSourceType;
    seek(to: SeqPosEnum): Promise<bigint | FileErr>;
    switchTo(type: FileSourceType): DynFileSource;
    getOffset(): bigint;
    fileSize(): bigint;
    setTimeout(ms: number): void;
}