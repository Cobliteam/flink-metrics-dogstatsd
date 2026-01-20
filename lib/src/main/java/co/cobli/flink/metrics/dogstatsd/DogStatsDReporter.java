package co.cobli.flink.metrics.dogstatsd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink Metric Reporter that sends metrics via the DogStatsD protocol (UDP).
 */
public class DogStatsDReporter
  implements MetricReporter, Scheduled, CharacterFilter
{

  private static final Logger LOG = LoggerFactory.getLogger(
    DogStatsDReporter.class
  );

  // --- Configuration Constants ---
  private static final String HOST_PARAM = "host";
  private static final String PORT_PARAM = "port";
  private static final String TAGS_PARAM = "tags";
  private static final String SHORT_IDS_PARAM = "shortIds";

  // --- Regex Patterns ---
  private static final Pattern INSTANCE_REF = Pattern.compile("@[a-f0-9]+");
  private static final Pattern FLINK_ID = Pattern.compile("[a-f0-9]{32}");

  // Metric Names: Alphanumeric, Underscore, Dot.
  private static final Pattern UNSAFE_NAME_CHARS = Pattern.compile(
    "[^a-zA-Z0-9_.]"
  );
  private static final Pattern MULTI_UNDERSCORE = Pattern.compile("_+");
  // Ensure name starts with a letter
  private static final Pattern STARTS_WITH_DIGIT = Pattern.compile("^[0-9]");

  // Tag Keys: Strictly Alphanumeric, Underscore, Dot, Dash
  private static final Pattern TAG_KEY_UNSAFE = Pattern.compile(
    "[^a-zA-Z0-9_.-]"
  );

  // Tag Values: Allow Alphanumeric, Underscore, Dot, Dash, Slash.
  // EVERYTHING ELSE (Spaces, Commas, Pipes, Colons) -> Underscore
  private static final Pattern TAG_VALUE_UNSAFE = Pattern.compile(
    "[^a-zA-Z0-9_./-]"
  );

  // --- Networking ---
  private volatile DatagramChannel channel;
  private static final int MAX_PACKET_SIZE = 1432;
  private final ByteBuffer buffer = ByteBuffer.allocateDirect(MAX_PACKET_SIZE);

  // --- State ---
  private List<String> globalTags = Collections.emptyList();
  private boolean useShortIds = false;

  // Tracks previous values for Counters/Histograms to calculate deltas.
  // Keyed by the Metric instance to avoid name collision issues.
  private final Map<Counter, Long> previousCounterValues =
    new ConcurrentHashMap<>();
  private final Map<Histogram, Long> previousHistogramCounts =
    new ConcurrentHashMap<>();

  // --- Metrics for the Reporter itself ---
  private final AtomicLong droppedOversize = new AtomicLong(0);
  private final AtomicLong sendErrors = new AtomicLong(0);
  private final AtomicLong invalidTags = new AtomicLong(0);
  private long lastTelemetryLog = System.currentTimeMillis();

  /**
   * Container to hold pre-calculated metadata.
   * We compute cleanName and tags ONCE when metric is added.
   */
  private static class DMetric {

    final String cleanName;
    final String tagsSuffix;

    DMetric(String cleanName, String tagsSuffix) {
      this.cleanName = cleanName;
      this.tagsSuffix = tagsSuffix;
    }
  }

  // Typed maps for faster iteration (no casting needed in report loop)
  protected final Map<Gauge<?>, DMetric> gauges = new ConcurrentHashMap<>();
  protected final Map<Counter, DMetric> counters = new ConcurrentHashMap<>();
  protected final Map<Histogram, DMetric> histograms =
    new ConcurrentHashMap<>();
  protected final Map<Meter, DMetric> meters = new ConcurrentHashMap<>();

  @Override
  public void open(MetricConfig config) {
    String host = config.getString(HOST_PARAM, "localhost");
    int port = config.getInteger(PORT_PARAM, 8125);
    String rawTags = config.getString(TAGS_PARAM, "");
    this.useShortIds = config.getBoolean(SHORT_IDS_PARAM, true);

    // Sanitize global tags immediately to prevent protocol errors
    this.globalTags = parseAndSanitizeTags(rawTags);

    if (this.channel != null) {
      close();
    }

    try {
      buffer.clear();
      InetSocketAddress address = new InetSocketAddress(host, port);
      DatagramChannel newChannel = DatagramChannel.open();
      newChannel.connect(address);
      this.channel = newChannel;
      LOG.info(
        "DogStatsDReporter started. Target: {}:{}, ShortIds: {}",
        host,
        port,
        useShortIds
      );
    } catch (IOException e) {
      LOG.error("Could not open/connect UDP channel", e);
    }
  }

  @Override
  public void close() {
    DatagramChannel ch = this.channel;
    this.channel = null;
    if (ch != null) {
      try {
        flush(ch);
        ch.close();
      } catch (IOException e) {
        LOG.warn("Error closing UDP channel", e);
      }
    }
    previousCounterValues.clear();
    previousHistogramCounts.clear();
  }

  // --- Metric Registration ---

  @Override
  public void notifyOfAddedMetric(
    Metric metric,
    String metricName,
    MetricGroup group
  ) {
    // 1. Calculate Logical Name
    String logicalName = group.getMetricIdentifier(metricName, this);

    // 2. Filter/Clean the name
    String cleanName = filterCharacters(logicalName);

    // 3. Build Tags (Global + Group)
    List<String> combinedTags = new ArrayList<>(globalTags);
    combinedTags.addAll(getTagsFromMetricGroup(group));
    String tagSuffix = combinedTags.isEmpty()
      ? ""
      : "|#" + String.join(",", combinedTags);

    DMetric dMetric = new DMetric(cleanName, tagSuffix);

    if (metric instanceof Counter) {
      counters.put((Counter) metric, dMetric);
    } else if (metric instanceof Gauge) {
      gauges.put((Gauge<?>) metric, dMetric);
    } else if (metric instanceof Histogram) {
      histograms.put((Histogram) metric, dMetric);
    } else if (metric instanceof Meter) {
      meters.put((Meter) metric, dMetric);
    } else {
      LOG.warn("Unsupported metric type: {}", metric.getClass().getName());
    }
  }

  @Override
  public void notifyOfRemovedMetric(
    Metric metric,
    String metricName,
    MetricGroup group
  ) {
    if (metric instanceof Counter) {
      counters.remove(metric);
      previousCounterValues.remove(metric);
    } else if (metric instanceof Gauge) {
      gauges.remove(metric);
    } else if (metric instanceof Histogram) {
      histograms.remove(metric);
      previousHistogramCounts.remove(metric);
    } else if (metric instanceof Meter) {
      meters.remove(metric);
    }
  }

  // --- Reporting Loop ---

  @Override
  public void report() {
    DatagramChannel ch = this.channel;
    if (ch == null || !ch.isOpen()) return;

    try {
      for (Map.Entry<Gauge<?>, DMetric> entry : gauges.entrySet()) {
        reportGauge(ch, entry.getValue(), entry.getKey());
      }
      for (Map.Entry<Counter, DMetric> entry : counters.entrySet()) {
        reportCounter(ch, entry.getValue(), entry.getKey());
      }
      for (Map.Entry<Histogram, DMetric> entry : histograms.entrySet()) {
        reportHistogram(ch, entry.getValue(), entry.getKey());
      }
      for (Map.Entry<Meter, DMetric> entry : meters.entrySet()) {
        reportMeter(ch, entry.getValue(), entry.getKey());
      }

      flush(ch);
      cleanupState();
      logTelemetry();
    } catch (Exception e) {
      sendErrors.incrementAndGet();
    }
  }

  private void reportGauge(
    DatagramChannel ch,
    DMetric dMetric,
    Gauge<?> gauge
  ) {
    Object value = gauge.getValue();
    if (value instanceof Number) {
      bufferMetric(
        ch,
        dMetric.cleanName,
        value.toString(),
        "g",
        dMetric.tagsSuffix
      );
    }
  }

  private void reportCounter(
    DatagramChannel ch,
    DMetric dMetric,
    Counter counter
  ) {
    // Atomic update via compute() to ensure correct delta calculation
    previousCounterValues.compute(counter, (k, previous) -> {
      long current = k.getCount();
      if (previous == null) return current; // First seen: set baseline, no emit

      long delta = current - previous;
      if (delta > 0) {
        bufferMetric(
          ch,
          dMetric.cleanName,
          String.valueOf(delta),
          "c",
          dMetric.tagsSuffix
        );
      }
      return current;
    });
  }

  private void reportHistogram(
    DatagramChannel ch,
    DMetric dMetric,
    Histogram histogram
  ) {
    HistogramStatistics stats = histogram.getStatistics();
    if (stats == null) return;

    // 1. Report Count as Delta
    // We use .count with type |c so Datadog aggregates it as "samples per interval".
    previousHistogramCounts.compute(histogram, (k, previous) -> {
      long current = k.getCount();
      if (previous == null) return current;

      long delta = current - previous;
      if (delta > 0) {
        bufferMetric(
          ch,
          dMetric.cleanName + ".count",
          String.valueOf(delta),
          "c",
          dMetric.tagsSuffix
        );
      }
      return current;
    });

    // 2. Report Distribution Stats as Gauges
    bufferMetric(
      ch,
      dMetric.cleanName + ".p95",
      String.valueOf(stats.getQuantile(0.95)),
      "g",
      dMetric.tagsSuffix
    );
    bufferMetric(
      ch,
      dMetric.cleanName + ".p99",
      String.valueOf(stats.getQuantile(0.99)),
      "g",
      dMetric.tagsSuffix
    );
    bufferMetric(
      ch,
      dMetric.cleanName + ".max",
      String.valueOf(stats.getMax()),
      "g",
      dMetric.tagsSuffix
    );
    bufferMetric(
      ch,
      dMetric.cleanName + ".mean",
      String.valueOf(stats.getMean()),
      "g",
      dMetric.tagsSuffix
    );
  }

  private void reportMeter(DatagramChannel ch, DMetric dMetric, Meter meter) {
    // Rate is a Gauge
    bufferMetric(
      ch,
      dMetric.cleanName + ".rate",
      String.valueOf(meter.getRate()),
      "g",
      dMetric.tagsSuffix
    );
    // Total Count is a Gauge ("current total") - stricter than delta tracking for secondary value
    bufferMetric(
      ch,
      dMetric.cleanName + ".count_total",
      String.valueOf(meter.getCount()),
      "g",
      dMetric.tagsSuffix
    );
  }

  // --- Buffering & I/O ---

  private void bufferMetric(
    DatagramChannel ch,
    String name,
    String value,
    String type,
    String tagsSuffix
  ) {
    try {
      int len = name.length() + value.length() + tagsSuffix.length() + 10;
      StringBuilder sb = new StringBuilder(len);
      sb
        .append(name)
        .append(':')
        .append(value)
        .append('|')
        .append(type)
        .append(tagsSuffix)
        .append('\n');

      byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);

      if (data.length > buffer.capacity()) {
        droppedOversize.incrementAndGet();
        return;
      }

      if (buffer.position() + data.length > buffer.capacity()) {
        flush(ch);
      }
      buffer.put(data);
    } catch (IOException e) {
      sendErrors.incrementAndGet();
    }
  }

  private void flush(DatagramChannel ch) throws IOException {
    if (ch == null || !ch.isOpen()) {
      buffer.clear();
      return;
    }
    if (buffer.position() > 0) {
      buffer.flip();
      try {
        writeToChannel(ch, buffer);
      } catch (IOException e) {
        sendErrors.incrementAndGet();
        throw e;
      } finally {
        buffer.clear();
      }
    }
  }

  protected void writeToChannel(DatagramChannel ch, ByteBuffer buffer)
    throws IOException {
    ch.write(buffer);
  }

  private void cleanupState() {
    // Remove state for metrics that no longer exist in our maps
    Set<Counter> liveCounters = counters.keySet();
    previousCounterValues
      .entrySet()
      .removeIf(e -> !liveCounters.contains(e.getKey()));

    Set<Histogram> liveHistograms = histograms.keySet();
    previousHistogramCounts
      .entrySet()
      .removeIf(e -> !liveHistograms.contains(e.getKey()));
  }

  // --- Helpers: Sanitization & Metadata ---

  @Override
  public String filterCharacters(String input) {
    if (input == null) return "flink_metric";

    // 1. Remove instance references (@7b1d3a)
    String clean = INSTANCE_REF.matcher(input).replaceAll("");

    // 2. Shorten Flink IDs
    clean = shortenIds(clean);

    // 3. Normalize unsafe characters
    clean = UNSAFE_NAME_CHARS.matcher(clean).replaceAll("_");
    clean = MULTI_UNDERSCORE.matcher(clean).replaceAll("_");

    // 4. Recursive trim of separators
    while (clean.startsWith("_") || clean.startsWith(".")) {
      if (clean.length() == 1) {
        clean = "";
        break;
      }
      clean = clean.substring(1);
    }
    while (clean.endsWith("_") || clean.endsWith(".")) {
      if (clean.length() == 1) {
        clean = "";
        break;
      }
      clean = clean.substring(0, clean.length() - 1);
    }

    // 5. Handle empty / invalid start
    if (clean.isEmpty()) {
      return "flink_metric";
    }
    if (STARTS_WITH_DIGIT.matcher(clean).find()) {
      clean = "m_" + clean;
    }

    // 6. Global compression
    if (clean.length() > 200) {
      // Hash BEFORE truncating to preserve uniqueness
      int h = clean.hashCode();
      clean = clean.substring(0, 190) + "_" + Integer.toHexString(h);
    }

    return clean;
  }

  private List<String> getTagsFromMetricGroup(MetricGroup metricGroup) {
    List<String> tags = new ArrayList<>();
    for (Map.Entry<String, String> entry : metricGroup
      .getAllVariables()
      .entrySet()) {
      String key = stripBrackets(entry.getKey());
      String val = entry.getValue();

      key = TAG_KEY_UNSAFE.matcher(key).replaceAll("_");
      if (key.isEmpty()) {
        invalidTags.incrementAndGet();
        continue;
      }

      // Process pipeline: shorten IDs -> strict whitelist
      val = shortenIds(val);
      val = sanitizeTagValue(val);

      tags.add(key + ":" + val);
    }
    return tags;
  }

  /**
   * Helper to shorten Flink IDs (32-char hex) to 8 chars.
   */
  private String shortenIds(String input) {
    if (!useShortIds || input == null) return input;

    Matcher m = FLINK_ID.matcher(input);
    if (!m.find()) return input;

    m.reset();
    StringBuffer sb = new StringBuffer(input.length());
    while (m.find()) {
      String repl = m.group().substring(0, 8);
      m.appendReplacement(sb, Matcher.quoteReplacement(repl));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  private String sanitizeTagValue(String val) {
    if (val == null) return "";
    return TAG_VALUE_UNSAFE.matcher(val).replaceAll("_");
  }

  private String stripBrackets(String str) {
    if (str.startsWith("<") && str.endsWith(">")) {
      return str.substring(1, str.length() - 1);
    }
    return str;
  }

  /**
   * Parse global tags. Sanitizes keys/values.
   * DROPS bare tags and empty keys.
   */
  private List<String> parseAndSanitizeTags(String rawTags) {
    if (rawTags == null || rawTags.isEmpty()) return Collections.emptyList();

    String[] parts = rawTags.split(",");
    List<String> valid = new ArrayList<>();

    for (String part : parts) {
      String trimmed = part.trim();
      if (trimmed.isEmpty()) continue;

      int idx = trimmed.indexOf(':');
      if (idx > 0) {
        // key:value pair
        String key = trimmed.substring(0, idx);
        String val = trimmed.substring(idx + 1);

        key = TAG_KEY_UNSAFE.matcher(key).replaceAll("_");
        if (key.isEmpty()) {
          invalidTags.incrementAndGet();
          continue;
        }

        val = sanitizeTagValue(val);
        valid.add(key + ":" + val);
      } else {
        // Bare tags are dropped for strict compliance
        invalidTags.incrementAndGet();
      }
    }
    return valid;
  }

  private void logTelemetry() {
    long now = System.currentTimeMillis();
    if (now - lastTelemetryLog > 60000) {
      long drops = droppedOversize.getAndSet(0);
      long errors = sendErrors.getAndSet(0);
      long badTags = invalidTags.getAndSet(0);

      if (drops > 0 || errors > 0 || badTags > 0) {
        LOG.warn(
          "DogStatsD Telemetry (Last 1m): dropped_oversize={}, send_errors={}, invalid_tags={}",
          drops,
          errors,
          badTags
        );
      }
      lastTelemetryLog = now;
    }
  }
}
