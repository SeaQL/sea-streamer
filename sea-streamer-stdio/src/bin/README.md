# SeaStreamer Stdio Utils

## clock

+ Generate a stream of ticks

Usage: 

```shell
cargo run --bin clock -- --interval 1s --stream clock

[2022-12-06T18:31:23.285852 | clock | 0] { "tick": 0 }
[2022-12-06T18:31:24.287452 | clock | 1] { "tick": 1 }
[2022-12-06T18:31:25.289144 | clock | 2] { "tick": 2 }
[2022-12-06T18:31:26.289846 | clock | 3] { "tick": 3 }
```

## relay

+ Stream from input and redirect to output (not really useful without filters)

Usage (using unix pipe):

```shell
cargo run --bin clock -- --interval 1s --stream clock | cargo run --bin relay -- --input clock --output relay

[2022-12-06T18:34:19.348575 | relay | 0] {"relay":true,"tick":0}
[2022-12-06T18:34:20.351214 | relay | 1] {"relay":true,"tick":1}
[2022-12-06T18:34:21.353177 | relay | 2] {"relay":true,"tick":2}
[2022-12-06T18:34:22.354132 | relay | 3] {"relay":true,"tick":3}
```

## complex

```shell
cargo run --bin clock -- --interval 1s --stream clock | cargo run --bin complex -- --input clock --output relay

[2023-02-24T08:13:10Z DEBUG sea_streamer_stdio::producer] [57811] stdout thread spawned
[2023-02-24T08:13:10 | relay | 0] {"relay":1,"tick":0}
[2023-02-24T08:13:11 | relay | 1] {"relay":2,"tick":1}
[2023-02-24T08:13:12 | relay | 2] {"relay":1,"tick":2}
[2023-02-24T08:13:13 | relay | 3] {"relay":2,"tick":3}
[2023-02-24T08:13:14 | relay | 4] {"relay":1,"tick":4}
[2023-02-24T08:13:15 | relay | 5] {"relay":2,"tick":5}
[2023-02-24T08:13:16 | relay | 6] {"relay":1,"tick":6}
[2023-02-24T08:13:17 | relay | 7] {"relay":2,"tick":7}
[2023-02-24T08:13:18 | relay | 8] {"relay":1,"tick":8}
[2023-02-24T08:13:19 | relay | 9] {"relay":2,"tick":9}
[2023-02-24T08:13:19Z DEBUG sea_streamer_stdio::producer] [57812] stdout thread exit
[2023-02-24T08:13:19Z DEBUG sea_streamer_stdio::producer] [57812] stdout thread spawned
[2023-02-24T08:13:20 | relay | 10] {"relay":0,"tick":10}
[2023-02-24T08:13:21 | relay | 11] {"relay":0,"tick":11}
[2023-02-24T08:13:22 | relay | 12] {"relay":0,"tick":12}
[2023-02-24T08:13:23 | relay | 13] {"relay":0,"tick":13}
[2023-02-24T08:13:24 | relay | 14] {"relay":0,"tick":14}
[2023-02-24T08:13:25 | relay | 15] {"relay":0,"tick":15}
[2023-02-24T08:13:26 | relay | 16] {"relay":0,"tick":16}
[2023-02-24T08:13:27 | relay | 17] {"relay":0,"tick":17}
[2023-02-24T08:13:28 | relay | 18] {"relay":0,"tick":18}
[2023-02-24T08:13:29 | relay | 19] {"relay":0,"tick":19}
[2023-02-24T08:13:30 | relay | 20] {"relay":0,"tick":20}
```