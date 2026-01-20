# Flink DogStatsD Metric Reporter

A lightweight, robust custom [Apache Flink](https://flink.apache.org/) Metric Reporter that sends metrics to Datadog via the **DogStatsD** protocol (UDP).

This reporter is designed for high-throughput production environments. It handles UDP batching, proper metric name sanitization, and failure recovery semantics (at-most-once delivery).

## Building

Requirements:

- Java 8 or 11
- Gradle 7+ (Wrapper included)

Run the build command:

```bash
./gradlew clean :lib:assemble
```

The compiled JAR will be located at: `lib/build/libs/flink-metrics-dogstatsd-1.x.x.jar`

## Installation

1. **Copy the JAR:** Place the built JAR file into the `lib/` directory of your Flink installation. This must be done on **all** nodes (JobManager and TaskManagers).

2. **Restart Cluster:** Restart Flink processes to load the new class.

## Configuration

Add the following configuration to your `flink-conf.yaml`:

```yaml
metrics.reporters: dogstatsd
metrics.reporter.dogstatsd.class: co.cobli.flink.metrics.dogstatsd.DogStatsDReporter
# or metrics.reporter.dogstatsd.factory.class: co.cobli.flink.metrics.dogstatsd.DogStatsDReporterFactory
metrics.reporter.dogstatsd.host: localhost # DogStatsD Agent IP
metrics.reporter.dogstatsd.port: 8125

# Recommended: Set it explicitly
metrics.reporter.dogstatsd.interval: 10 SECONDS

# Optional: global tags (k:v only)
metrics.reporter.dogstatsd.tags: env:production,team:data

# Optional: handle ID shortening (default: true)
metrics.reporter.dogstatsd.shortIds: true
```

## Internal Telemetry

The reporter monitors its own health and logs warnings if it detects issues (throttled to once per minute):

- `dropped_oversize`: Metrics larger than the buffer (1432 bytes) were dropped.
- `send_errors`: UDP send failures.
- `invalid_tags`: Tags dropped due to format errors (e.g., bare tags, empty keys).

## Metric Schema

| Flink Metric  | DogStatsD Type  | Suffix         | Description                                |
| :------------ | :-------------- | :------------- | :----------------------------------------- |
| **Counter**   | Counter (`\|c`) | (none)         | Delta value (count since last report).     |
| **Gauge**     | Gauge (`\|g`)   | (none)         | Instantaneous value.                       |
| **Meter**     | Gauge (`\|g`)   | `.rate`        | Events per second.                         |
| **Meter**     | Gauge (`\|g`)   | `.count_total` | Total events seen (monotonic).             |
| **Histogram** | Counter (`\|c`) | `.count`       | Number of events in this interval (Delta). |
| **Histogram** | Gauge (`\|g`)   | `.p95`, `.max` | Distribution statistics.                   |
