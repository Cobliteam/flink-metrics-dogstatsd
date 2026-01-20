# Architecture & Design Decisions

This document outlines the architectural choices and design principles behind the `DogStatsDReporter`.

## 1. Networking & I/O

- **Protocol:** We use **UDP** via Java NIO `DatagramChannel`.
- **Batching:** Instead of sending one UDP packet per metric (which causes high syscall overhead), we buffer metrics into a `ByteBuffer` of **1432 bytes** (standard safe MTU size).
- **Flush Strategy:** The buffer is flushed immediately when it fills up or when the reporting loop finishes.

## 2. Sanitization Strategy

We employ a **strict allow-list** approach to ensure compatibility with Datadog and prevent protocol corruption.

- **Metric Names:**
  - **Allowed:** Alphanumeric, `_`, `.`
  - **Normalization:** All other characters are replaced with `_`.
  - **Trimming:** Recursive removal of leading/trailing `.` or `_` to prevent invalid paths (e.g., `..my_metric`).
  - **Compression:** Names > 200 chars are hashed (before truncation) to preserve uniqueness while preventing downstream truncation issues.
  - **Fallback:** If a name becomes empty or invalid, it defaults to `"flink_metric"` to ensure visibility.

- **Tags:**
  - **Strict Key/Value:** Bare tags (e.g., `production`) are **dropped**. Only `key:value` pairs are accepted.
  - **Sanitization:** Tag values are strictly sanitized to remove DogStatsD separators (`|`, `,`, `#`).
  - **Empty Keys:** If a tag key becomes empty after sanitization, the tag is dropped to prevent malformed packets.

## 3. Flink Integration & Cardinality Control

Flink jobs often generate high-cardinality identifiers that can explode Datadog costs.

- **ID Shortening:** 32-character hex IDs (common in Flink task IDs) are detected and shortened to their first **8 characters** (e.g., `a1b2c3d4`).
- **Instance References:** Object references (e.g., `@7b1d3a`) are stripped entirely to ensure metric names are stable across JVM restarts.

## 4. State Management (Delta Tracking)

Flink Counters and Histograms are stateful, but DogStatsD expects:

- **Counters (`|c`):** Deltas (counts per interval).
- **Gauges (`|g`):** Absolute values.

To bridge this:

- **Atomic Updates:** We use `ConcurrentHashMap.compute()` to calculate deltas atomically. This prevents race conditions if the reporter is accessed concurrently.
- **Identity Keying:** We track state using the **Metric Object Instance** (`Counter` object), not the metric name. This prevents collisions if two metrics happen to share a name but are distinct objects.
- **Leak Prevention:** A `cleanupState()` routine runs at the end of every report cycle to remove state for metrics that are no longer present in the registry.

## 5. Metric Semantics

We adhere to **DogStatsD** semantics (an extension of StatsD with tagging support):

- **Meters:**
  - `.rate`: Reported as Gauge.
  - `.count_total`: Reported as Gauge (strictly monotonic). We avoid sending this as a delta counter to prevent confusion about resets.
- **Histograms:**
  - `.count`: Reported as a **Counter (|c)** (Delta). This allows Datadog to correctly compute "events per second" aggregations.
  - `p95`, `max`, `mean`: Reported as Gauges.
