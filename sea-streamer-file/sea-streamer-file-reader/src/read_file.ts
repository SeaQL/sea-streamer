import { AsyncFile } from './file';
import { SeqPos } from './types';

async function main() {
    const file = new AsyncFile("/home/chris/sea-streamer/sea-streamer-file/sea-streamer-file-reader/testcases/consumer.ss");

    await file.openRead();

    while (true) {
        const buffer = await file.read();
        if (buffer.length == 0) {
            break;
        }
        console.log(buffer.toString());
    }

    const res = await file.seek(new SeqPos.At(1000000000000n));
    console.log(res);

    await file.close();
}

main();