# SeaStreamer Benchmark

Some micro benchmarks. Note: it is only meaningful for these numbers to compare with itself. It's only intended for measuring the performance improvements (and hopefully not regressions) over the course of development.

## Commands

```sh
time cargo run --release --bin baseline -- --stream stdio:///test

time cargo run --release --bin producer -- --stream redis://localhost/clock
rm clock.ss; touch clock.ss; time cargo run --release --bin producer -- --stream file://clock.ss/clock
time cargo run --release --bin producer -- --stream stdio:///clock > clock.log

time cargo run --release --bin consumer -- --stream redis://localhost/clock
time cargo run --release --bin consumer -- --stream file://clock.ss/clock
time ( cargo run --release --bin producer -- --stream stdio:///clock | cargo run --release --bin relay -- --input clock --output relay | cargo run --release --bin consumer -- --stream stdio:///relay )
```

## Summary

100k messages.

Each message has a payload of 256 bytes.

Dump is around 30MB in size.