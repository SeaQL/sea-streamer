# 🌊 SeaStreamer Kafka / Redpanda Backend

To setup a Redpanda instance:

```sh
docker run -d --pull=always --name=redpanda --rm \
    -p 8081:8081 \
    -p 8082:8082 \
    -p 9092:9092 \
    -p 9644:9644 \
    docker.redpanda.com/vectorized/redpanda:latest \
    redpanda start \
    --overprovisioned \
    --seeds "redpanda:33145" \
    --set redpanda.empty_seed_starts_cluster=false \
    --smp 1  \
    --memory 1G \
    --reserve-memory 0M \
    --check=false \
    --advertise-rpc-addr redpanda:33145
```

Then try:

```sh
docker exec CONTAINER_ID rpk topic list
```