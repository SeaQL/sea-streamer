type Meta = { [key: string]: string | number | boolean };

export class FileErr {
    type: FileErrType;
    meta: Meta | undefined;

    constructor(type: FileErrType, meta?: Meta) {
        this.type = type;
        this.meta = meta;
    }
}

export enum FileErrType {
    NotEnoughBytes = "NotEnoughBytes",
    FormatErr__ByteMark = "FormatErr::ByteMark",
    FormatErr__Version = "FormatErr::Version",
    FormatErr__ChecksumErr = "FormatErr::ChecksumErr",
}