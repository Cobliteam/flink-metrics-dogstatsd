package co.cobli.flink.metrics.dogstatsd;

import java.util.Properties;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

public class DogStatsDReporterFactory implements MetricReporterFactory {

  @Override
  public MetricReporter createMetricReporter(Properties properties) {
    return new DogStatsDReporter();
  }
}
