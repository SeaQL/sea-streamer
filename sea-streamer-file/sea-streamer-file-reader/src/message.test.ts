import { FileErr, FileErrType } from "./error";
import { MessageSource, isEndOfStream } from "./message";
import { SeqPos, StreamMode } from "./types";

test('rewind', async () => testRewind(StreamMode.Replay));

test('rewind', async () => testRewind(StreamMode.LiveReplay));

async function testRewind(mode: StreamMode) {
    const path = './testcases/consumer.ss';
    const source = await MessageSource.new(path, mode);
    if (source instanceof FileErr) { throwNewError(source); }

    await expectNext(0, 100);

    let nth;
    nth = await source.rewind(new SeqPos.At(1n)); if (nth instanceof FileErr) { throwNewError(nth); }
    expect(nth).toStrictEqual(1);

    await expectNext(21, 50);

    nth = await source.rewind(new SeqPos.At(4n)); if (nth instanceof FileErr) { throwNewError(nth); }
    expect(nth).toStrictEqual(4);

    await expectNext(86, 99);

    nth = await source.rewind(new SeqPos.Beginning); if (nth instanceof FileErr) { throwNewError(nth); }
    expect(nth).toStrictEqual(0);

    await expectNext(0, 100);

    const eos = await source.next(); if (eos instanceof FileErr) { throwNewError(eos); }
    expect(isEndOfStream(eos)).toBe(true);

    if (mode === StreamMode.Replay) {
        const err = await source.next();
        expect(err).toStrictEqual(new FileErr(FileErrType.NotEnoughBytes));
    } else if (mode === StreamMode.LiveReplay) {
        source.setTimeout(100);
        const err = await source.next();
        expect(err).toStrictEqual(new FileErr(FileErrType.TimedOut));
    }

    async function expectNext(from: number, to: number) {
        if (source instanceof FileErr) { throwNewError(source); }
        for (let i = from; i < to; i++) {
            const message = await source.next(); if (message instanceof FileErr) { throwNewError(message); }
            expect(message.payload.toString()).toStrictEqual(`hello-${i}`);
        }
    }
}

function throwNewError(err: FileErr): never {
    throw new Error(err.toString());
}
