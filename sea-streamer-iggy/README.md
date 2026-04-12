### `sea-streamer-iggy`: Iggy Backend

This is the Iggy backend implementation for SeaStreamer.

## Tests

To run the tests, you need to have an Iggy server running. You can start one by running the following command:

```bash
docker run -d -p 8090:8090 \
--cap-add=SYS_NICE \
--security-opt seccomp=unconfined \
--ulimit memlock=-1:-1 \
-e IGGY_ROOT_USERNAME=iggy \
-e IGGY_ROOT_PASSWORD=iggy \
-e IGGY_TCP_ENABLED=true \
-e IGGY_TCP_ADDRESS=0.0.0.0:8090 \
-e IGGY_SYSTEM_SHARDING_CPU_ALLOCATION=1 \
apache/iggy:latest
```

Then, you can run the tests by running the following command:

```bash
cargo test --package sea-streamer-iggy --features test,runtime-tokio -- --nocapture
```

