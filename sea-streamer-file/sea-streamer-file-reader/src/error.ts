type Meta = { [key: string]: string | number | boolean };

export class FileErr {
    type: FileErrType;
    meta: Meta | undefined;

    constructor(type: FileErrType, meta?: Meta) {
        this.type = type;
        this.meta = meta;
    }

    toString(): string {
        return `${this.type}${this.meta !== undefined ? ": " + this.meta : ""}`;
    }
}

export enum FileErrType {
    TimedOut = "TimedOut",
    NotEnoughBytes = "NotEnoughBytes",
    FormatErr__ByteMark = "FormatErr::ByteMark",
    FormatErr__Version = "FormatErr::Version",
    FormatErr__ChecksumErr = "FormatErr::ChecksumErr",
}