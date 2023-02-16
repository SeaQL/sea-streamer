# SeaStreamer Stdio Utils

## clock

+ Generate a stream of ticks

Usage: 

```sh
cargo run --bin clock -- --interval 1s --stream clock

[2022-12-06T18:31:23.285852 | clock | 0] { "tick": 0 }
[2022-12-06T18:31:24.287452 | clock | 1] { "tick": 1 }
[2022-12-06T18:31:25.289144 | clock | 2] { "tick": 2 }
[2022-12-06T18:31:26.289846 | clock | 3] { "tick": 3 }
```

## relay

+ Stream from input and redirect to output (not really useful without filters)

Usage (using unix pipe):

```sh
cargo run --bin clock -- --interval 1s --stream clock | cargo run --bin relay -- --input clock --output relay

[2022-12-06T18:34:19.348575 | relay | 0] {"relay":true,"tick":0}
[2022-12-06T18:34:20.351214 | relay | 1] {"relay":true,"tick":1}
[2022-12-06T18:34:21.353177 | relay | 2] {"relay":true,"tick":2}
[2022-12-06T18:34:22.354132 | relay | 3] {"relay":true,"tick":3}
```

## complex

```sh
cargo run --bin clock -- --interval 1s --stream clock | cargo run --bin complex -- --input clock --output relay

[2023-02-10T15:47:08Z INFO  sea_streamer_stdio::consumers] [17469] stdin thread spawned
[2023-02-10T15:47:08Z INFO  sea_streamer_stdio::producer] [17469] stdout thread spawned
[2023-02-10T15:47:08Z INFO  sea_streamer_stdio::producer] [17468] stdout thread spawned
[2023-02-10T15:47:08.711493 | relay | 0] {"relay":1,"tick":0} // messages are load balanced
[2023-02-10T15:47:09.713421 | relay | 1] {"relay":2,"tick":1} // messages are load balanced
[2023-02-10T15:47:10.715063 | relay | 2] {"relay":1,"tick":2}
[2023-02-10T15:47:11.718128 | relay | 3] {"relay":2,"tick":3}
[2023-02-10T15:47:12.720266 | relay | 4] {"relay":1,"tick":4}
[2023-02-10T15:47:13.722050 | relay | 5] {"relay":2,"tick":5}
[2023-02-10T15:47:14.724059 | relay | 6] {"relay":1,"tick":6}
[2023-02-10T15:47:15.725480 | relay | 7] {"relay":2,"tick":7}
[2023-02-10T15:47:16.727002 | relay | 8] {"relay":1,"tick":8}
[2023-02-10T15:47:17.728115 | relay | 9] {"relay":2,"tick":9}
[2023-02-10T15:47:17Z INFO  sea_streamer_stdio::producer] [17469] stdout thread exit
[2023-02-10T15:47:18Z INFO  sea_streamer_stdio::consumers] [17469] stdin thread exit
// there might be data loss
[2023-02-10T15:47:18Z INFO  sea_streamer_stdio::producer] [17469] stdout thread spawned
[2023-02-10T15:47:18Z INFO  sea_streamer_stdio::consumers] [17469] stdin thread spawned
[2023-02-10T15:47:19.730481 | relay | 10] {"relay":0,"tick":11}
[2023-02-10T15:47:20.732021 | relay | 11] {"relay":0,"tick":12}
[2023-02-10T15:47:21.734525 | relay | 12] {"relay":0,"tick":13}
[2023-02-10T15:47:22.736043 | relay | 13] {"relay":0,"tick":14}
[2023-02-10T15:47:23.738345 | relay | 14] {"relay":0,"tick":15}
[2023-02-10T15:47:24.742351 | relay | 15] {"relay":0,"tick":16}
[2023-02-10T15:47:25.744534 | relay | 16] {"relay":0,"tick":17}
[2023-02-10T15:47:26.746190 | relay | 17] {"relay":0,"tick":18}
[2023-02-10T15:47:27.748010 | relay | 18] {"relay":0,"tick":19}
[2023-02-10T15:47:28.748867 | relay | 19] {"relay":0,"tick":20}
```