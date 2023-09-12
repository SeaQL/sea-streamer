import { FileErr } from "./error";
import { SeqPosEnum } from "./types";

export enum FileSourceType {
    FileReader = "FileReader",
    FileSource = "FileSource",
}

export interface DynFileSource {
    sourceType(): FileSourceType;
    seek(to: SeqPosEnum): Promise<bigint | FileErr>;
    resize(): Promise<void>;
    switchTo(type: FileSourceType): DynFileSource;
    getOffset(): bigint;
    fileSize(): bigint;
    setTimeout(ms: number): void;
    close(): Promise<void>;
}