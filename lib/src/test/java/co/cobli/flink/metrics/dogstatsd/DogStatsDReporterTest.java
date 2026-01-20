package co.cobli.flink.metrics.dogstatsd;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import org.apache.flink.metrics.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DogStatsDReporterTest {

  private static class TestableReporter extends DogStatsDReporter {

    volatile boolean shouldFailWrite = false;

    @Override
    protected void writeToChannel(DatagramChannel ch, ByteBuffer buffer)
      throws IOException {
      if (shouldFailWrite) {
        throw new IOException("Simulated Write Failure");
      }
      super.writeToChannel(ch, buffer);
    }
  }

  private static final int DEFAULT_TIMEOUT_MS = 1000;

  private TestableReporter reporter;
  private DatagramSocket serverSocket;
  private int serverPort;
  private MetricGroup mockGroup;

  @Before
  public void setup() throws Exception {
    startServer();

    reporter = new TestableReporter();
    MetricConfig config = new MetricConfig();
    config.setProperty("host", "127.0.0.1");
    config.setProperty("port", String.valueOf(serverPort));
    // Add a valid tag and an invalid bare tag "bare_tag" (should be dropped)
    config.setProperty("tags", "env:test,bare_tag");
    config.setProperty("shortIds", "true");
    reporter.open(config);

    mockGroup = mock(MetricGroup.class);

    when(
      mockGroup.getMetricIdentifier(anyString(), any(CharacterFilter.class))
    ).thenAnswer(invocation -> {
      String name = invocation.getArgument(0);
      CharacterFilter filter = invocation.getArgument(1);
      return filter.filterCharacters(name);
    });

    Map<String, String> variables = new HashMap<>();
    variables.put("<job_name>", "My Job");
    variables.put("<task_id>", "a1b2c3d4e5f600001111222233334444");
    when(mockGroup.getAllVariables()).thenReturn(variables);

    drainSocket(serverSocket);
  }

  private void startServer() throws Exception {
    serverSocket = new DatagramSocket(0);
    serverSocket.setSoTimeout(DEFAULT_TIMEOUT_MS);
    serverPort = serverSocket.getLocalPort();
  }

  @After
  public void teardown() {
    if (reporter != null) reporter.close();
    if (serverSocket != null && !serverSocket.isClosed()) serverSocket.close();
  }

  @Test
  public void testRichTagsAndSanitization() throws Exception {
    SimpleCounter c = new SimpleCounter();
    reporter.notifyOfAddedMetric(
      c,
      "flink.taskmanager.@123abcde.count",
      mockGroup
    );
    c.inc(5);
    reporter.report(); // Baseline
    c.inc(10);
    reporter.report(); // Delta

    List<String> lines = receiveAllLines(serverSocket, 1);
    if (lines.isEmpty()) fail("Received no packets for RichTags test");

    String packet = lines.get(0);

    assertTrue(
      "Name Sanitization Failed. Packet: " + packet,
      packet.contains("flink.taskmanager..count")
    );

    assertTrue(
      "Missing Global Tag. Packet: " + packet,
      packet.contains("env:test")
    );

    assertFalse(
      "Bare tag should have been dropped. Packet: " + packet,
      packet.contains("bare_tag")
    );

    assertTrue(
      "Missing Scope Tag (job_name). Packet: " + packet,
      packet.contains("job_name:My_Job")
    );

    assertTrue(
      "Task ID shortening failed. Expected 'task_id:a1b2c3d4'. Packet: " +
        packet,
      packet.contains("task_id:a1b2c3d4")
    );

    assertFalse(
      "Task ID leaked full 32 chars. Packet: " + packet,
      packet.contains("a1b2c3d4e5f600001111222233334444")
    );
  }

  @Test
  public void testCounterLifecycle() throws Exception {
    SimpleCounter counter = new SimpleCounter();
    reporter.notifyOfAddedMetric(counter, "test.counter", mockGroup);
    counter.inc(10);
    reporter.report();
    counter.inc(5);
    reporter.report();
    assertPacketContains("test.counter:5|c");
  }

  @Test
  public void testHistogramDeltaAndStats() throws Exception {
    Histogram hist = mock(Histogram.class);
    HistogramStatistics stats = mock(HistogramStatistics.class);
    when(hist.getCount()).thenReturn(100L);
    when(hist.getStatistics()).thenReturn(stats);
    when(stats.getQuantile(0.95)).thenReturn(50.0);
    when(stats.getMax()).thenReturn(100L);
    when(stats.getMean()).thenReturn(25.0);
    reporter.notifyOfAddedMetric(hist, "my.hist", mockGroup);
    reporter.report();
    List<String> lines = receiveAllLines(serverSocket, 1);
    assertTrue(lines.stream().anyMatch(l -> l.contains("my.hist.p95:50.0|g")));
    when(hist.getCount()).thenReturn(110L);
    reporter.report();
    lines = receiveAllLines(serverSocket, 1);
    assertTrue(lines.stream().anyMatch(l -> l.contains("my.hist.count:10|c")));
  }

  @Test
  public void testHistogramsAndMeters() throws Exception {
    Meter meter = mock(Meter.class);
    when(meter.getRate()).thenReturn(5.5);
    when(meter.getCount()).thenReturn(100L);
    reporter.notifyOfAddedMetric(meter, "my.meter", mockGroup);
    reporter.report();
    List<String> lines = receiveAllLines(serverSocket, 1);
    assertTrue(lines.stream().anyMatch(l -> l.contains("my.meter.rate:5.5|g")));
    assertTrue(
      lines.stream().anyMatch(l -> l.contains("my.meter.count_total:100|g"))
    );
  }

  @Test
  public void testAtMostOnceSemantics_Deterministic() throws Exception {
    SimpleCounter c = new SimpleCounter();
    reporter.notifyOfAddedMetric(c, "reliable.counter", mockGroup);
    c.inc(10);
    reporter.report();
    drainSocket(serverSocket);
    c.inc(1);
    reporter.report();
    assertPacketContains("reliable.counter:1|c");
    drainSocket(serverSocket);
    c.inc(5);
    reporter.shouldFailWrite = true;
    reporter.report();
    assertNoPacket("Sabotaged report");
    reporter.shouldFailWrite = false;
    reporter.report();
    assertNoPacket("Re-emit failure");
    c.inc(1);
    reporter.report();
    assertPacketContains("reliable.counter:1|c");
  }

  @Test
  public void testMultiPacketBatching() throws Exception {
    int count = 500;
    Set<String> expectedLines = new HashSet<>();
    for (int i = 0; i < count; i++) {
      final int val = i;
      String name = "batch.gauge.long.name.filling.space." + i;
      reporter.notifyOfAddedMetric((Gauge<Integer>) () -> val, name, mockGroup);
      expectedLines.add(name + ":" + i + "|g");
    }
    reporter.report();
    List<String> lines = receiveAllLines(serverSocket, count);
    assertTrue(lines.size() >= count);
  }

  @Test
  public void testReopenWithNewPort() throws Exception {
    DatagramSocket secondServer = new DatagramSocket(0);
    int secondPort = secondServer.getLocalPort();
    try {
      drainSocket(serverSocket);
      MetricConfig config = new MetricConfig();
      config.setProperty("host", "127.0.0.1");
      config.setProperty("port", String.valueOf(secondPort));
      reporter.open(config);
      drainSocket(secondServer);
      SimpleCounter c = new SimpleCounter();
      reporter.notifyOfAddedMetric(c, "reopen.test", mockGroup);
      c.inc(10);
      reporter.report();
      c.inc(5);
      reporter.report();
      List<String> lines = receiveAllLines(secondServer, 1);
      assertTrue(lines.stream().anyMatch(l -> l.contains("reopen.test:5|c")));
    } finally {
      secondServer.close();
    }
  }

  @Test
  public void testIdempotentClose() {
    SimpleCounter c = new SimpleCounter();
    reporter.notifyOfAddedMetric(c, "close.test", mockGroup);
    c.inc(1);
    reporter.report();
    reporter.close();
    reporter.close();
    reporter = null;
  }

  @Test
  public void testConcurrentModification() throws Exception {
    int count = 500;
    ExecutorService executor = Executors.newFixedThreadPool(2);
    for (int i = 0; i < count; i++) reporter.notifyOfAddedMetric(
      new SimpleCounter(),
      "static." + i,
      mockGroup
    );
    Future<?> reporterTask = executor.submit(() -> {
      for (int i = 0; i < 50; i++) {
        reporter.report();
        try {
          Thread.sleep(5);
        } catch (Exception e) {}
      }
    });
    Future<?> churnTask = executor.submit(() -> {
      for (int i = 0; i < 50; i++) {
        SimpleCounter c = new SimpleCounter();
        reporter.notifyOfAddedMetric(c, "churn." + i, mockGroup);
        try {
          Thread.sleep(1);
        } catch (Exception e) {}
        reporter.notifyOfRemovedMetric(c, "churn." + i, mockGroup);
      }
    });
    reporterTask.get(10, TimeUnit.SECONDS);
    churnTask.get(10, TimeUnit.SECONDS);
    executor.shutdownNow();
  }

  private void drainSocket(DatagramSocket socket) {
    byte[] buf = new byte[8192];
    try {
      socket.setSoTimeout(50);
      while (true) socket.receive(new DatagramPacket(buf, buf.length));
    } catch (Exception e) {
    } finally {
      try {
        socket.setSoTimeout(DEFAULT_TIMEOUT_MS);
      } catch (Exception e) {}
    }
  }

  private List<String> receiveAllLines(
    DatagramSocket socket,
    int expectedMinCount
  ) {
    List<String> lines = new ArrayList<>();
    byte[] buf = new byte[8192];
    DatagramPacket packet = new DatagramPacket(buf, buf.length);
    long deadline = System.currentTimeMillis() + 2000;
    int quietTimeouts = 0;
    try {
      while (System.currentTimeMillis() < deadline) {
        if (lines.size() >= expectedMinCount && quietTimeouts >= 2) break;
        try {
          socket.setSoTimeout(50);
          socket.receive(packet);
          quietTimeouts = 0;
          String raw = new String(
            packet.getData(),
            0,
            packet.getLength(),
            StandardCharsets.UTF_8
          );
          for (String line : raw.split("\n"))
            if (!line.trim().isEmpty()) lines.add(line.trim());
        } catch (SocketTimeoutException e) {
          quietTimeouts++;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        socket.setSoTimeout(DEFAULT_TIMEOUT_MS);
      } catch (Exception e) {}
    }
    return lines;
  }

  private void assertNoPacket(String message) {
    try {
      serverSocket.setSoTimeout(500);
      serverSocket.receive(new DatagramPacket(new byte[8192], 8192));
      fail(message);
    } catch (Exception e) {
    } finally {
      try {
        serverSocket.setSoTimeout(DEFAULT_TIMEOUT_MS);
      } catch (Exception e) {}
    }
  }

  private void assertPacketContains(String expectedFragment)
    throws IOException {
    List<String> lines = receiveAllLines(serverSocket, 1);
    if (lines.isEmpty()) fail(
      "Expected lines containing '" + expectedFragment + "' but got NOTHING."
    );
    if (lines.stream().noneMatch(l -> l.contains(expectedFragment))) fail(
      "Could not find '" +
        expectedFragment +
        "' in: " +
        String.join("\n", lines)
    );
  }
}
