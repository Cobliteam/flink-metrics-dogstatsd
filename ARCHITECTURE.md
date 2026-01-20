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

## 4. Stateless Design & Reliability (UDP Strategy)

We intentionally diverge from standard StatsD behavior by treating all metrics as **Stateless Absolute Values**.

- **The Problem:** Standard StatsD Counters send deltas (increments). Since UDP is "fire-and-forget", a single lost packet results in permanent data drift (e.g., total count is forever lower than reality). Additionally, during quiet periods (0 errors), standard reporters send nothing, causing "No Data" in alerts.
- **The Solution:** We report **Counters** and **Histogram Counts** as **Gauges (`|g`)** containing the absolute cumulative value from Flink.
  - **Self-Healing:** If a packet is lost, the next packet carries the correct total, correcting the graph immediately.
  - **Heartbeat:** We send the value even if it is `0`. This creates a continuous line in dashboards, distinguishing a "healthy system" from a "dead reporter".

## 5. Metric Semantics

We adhere to **DogStatsD** semantics but map Flink types to ensure robustness:

- **Counters:**
  - Reported as **Gauge (`|g`)**. Contains the total accumulated count.
- **Meters:**
  - `.rate`: Reported as **Gauge**.
  - `.count`: Reported as **Gauge**. (Renamed from `.count_total` to match standard conventions).
- **Histograms:**
  - `.count`: Reported as **Gauge (`|g`)**. (Absolute total). This prevents the "double count" risk associated with calculating deltas over UDP.
  - `p95`, `max`, `mean`: Reported as **Gauges**.
