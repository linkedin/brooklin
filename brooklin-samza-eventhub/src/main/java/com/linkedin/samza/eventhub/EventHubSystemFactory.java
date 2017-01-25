package com.linkedin.samza.eventhub;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;


public class EventHubSystemFactory implements SystemFactory {
  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new EventHubSystemProducer(systemName, config, registry);
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new EventHubSystemAdmin();
  }
}
