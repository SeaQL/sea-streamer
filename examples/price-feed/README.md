# Price Feed

This example demonstrates how to subscribe to a real-time websocket data feed and stream to Redis / Kafka.

As an example, we subscribe to the `GBP/USD` price feed from Kraken, documentation can be found at https://docs.kraken.com/websockets/#message-spread.

It will stream to localhost Redis by default. Stream key will be named `GBP_USD`.

```sh
cargo run
```

Here is a sample message serialized to JSON:

```json
{"spread":{"bid":"1.23150","ask":"1.23166","timestamp":"2024-04-22T11:24:41.461661","bid_vol":"40.55300552","ask_vol":"315.04699448"},"channel_name":"spread","pair":"GBP/USD"}
```

#### NOT FINANCIAL ADVICE: FOR EDUCATIONAL AND INFORMATIONAL PURPOSES ONLY