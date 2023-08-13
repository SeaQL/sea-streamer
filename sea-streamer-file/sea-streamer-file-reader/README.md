### `sea-streamer-file-reader`: File Reader for node.js

This library implements a decoder for the [SeaStreamer file format](https://github.com/SeaQL/sea-streamer/tree/main/sea-streamer-file).

It does not provide the high-level Consumer interface. But the reason it is so complex in Rust is due to multi-threaded concurrency. Since node.js is basically single threaded, this made the implementation much simpler.

#### Running the `decoder`

```sh
npm run decoder -- --file ./testcases/consumer.ss
```

```log
# {"fileName":"consumer-1691686154741","createdAt":"2023-08-10T16:49:14.742Z","beaconInterval":1024}
[2023-08-10T16:49:14.745Z | hello | 0 | 1] hello-0
[2023-08-10T16:49:14.746Z | hello | 1 | 1] hello-1
[2023-08-10T16:49:14.748Z | hello | 2 | 1] hello-2
[2023-08-10T16:49:14.750Z | hello | 3 | 1] hello-3
[2023-08-10T16:49:14.751Z | hello | 4 | 1] hello-4
[2023-08-10T16:49:14.753Z | hello | 5 | 1] hello-5
[2023-08-10T16:49:14.754Z | hello | 6 | 1] hello-6
[2023-08-10T16:49:14.755Z | hello | 7 | 1] hello-7
[2023-08-10T16:49:14.757Z | hello | 8 | 1] hello-8
[2023-08-10T16:49:14.758Z | hello | 9 | 1] hello-9
[2023-08-10T16:49:14.759Z | hello | 10 | 1] hello-10
[2023-08-10T16:49:14.761Z | hello | 11 | 1] hello-11
[2023-08-10T16:49:14.762Z | hello | 12 | 1] hello-12
[2023-08-10T16:49:14.763Z | hello | 13 | 1] hello-13
[2023-08-10T16:49:14.765Z | hello | 14 | 1] hello-14
[2023-08-10T16:49:14.766Z | hello | 15 | 1] hello-15
[2023-08-10T16:49:14.768Z | hello | 16 | 1] hello-16
[2023-08-10T16:49:14.769Z | hello | 17 | 1] hello-17
[2023-08-10T16:49:14.770Z | hello | 18 | 1] hello-18
[2023-08-10T16:49:14.771Z | hello | 19 | 1] hello-19
[2023-08-10T16:49:14.773Z | hello | 20 | 1] hello-20
# [{"header":{"streamKey":"hello","shardId":1,"sequence":20,"timestamp":"2023-08-10T16:49:14.773Z"},"runningChecksum":2887}]
[2023-08-10T16:49:14.775Z | hello | 21 | 1] hello-21
[2023-08-10T16:49:14.777Z | hello | 22 | 1] hello-22
[2023-08-10T16:49:14.778Z | hello | 23 | 1] hello-23
[2023-08-10T16:49:14.780Z | hello | 24 | 1] hello-24
[2023-08-10T16:49:14.781Z | hello | 25 | 1] hello-25
...
```