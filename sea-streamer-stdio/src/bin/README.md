# SeaStreamer Stdio Utils

## clock

+ Generate a stream of ticks

Usage: 

```sh
cargo run --bin clock -- --interval 1s --stream-key clock

[2022-12-06T18:31:23.285852 | clock | 0] { "tick": 0 }
[2022-12-06T18:31:24.287452 | clock | 1] { "tick": 1 }
[2022-12-06T18:31:25.289144 | clock | 2] { "tick": 2 }
[2022-12-06T18:31:26.289846 | clock | 3] { "tick": 3 }
```

## relay

+ Stream from input and redirect to output (not really useful without filters)

Usage (using unix pipe):

```sh
cargo run --bin clock -- --interval 1s --stream-key clock | cargo run --bin relay -- --input clock --output relay

[2022-12-06T18:34:19.348575 | relay | 0] {"relay":true,"tick":0}
[2022-12-06T18:34:20.351214 | relay | 1] {"relay":true,"tick":1}
[2022-12-06T18:34:21.353177 | relay | 2] {"relay":true,"tick":2}
[2022-12-06T18:34:22.354132 | relay | 3] {"relay":true,"tick":3}
```
