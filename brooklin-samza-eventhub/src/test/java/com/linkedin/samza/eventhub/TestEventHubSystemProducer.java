package com.linkedin.samza.eventhub;

import java.util.HashMap;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.testng.annotations.Test;

import junit.framework.Assert;


public class TestEventHubSystemProducer {

  public static final String EVENTHUB_NAMESPACE = "spunurutest2";
  public static final String EVENTHUB_NAME1 = "eventhubut1_p8";
  public static final String EVENTHUB_NAME2 = "eventhubut1_p4";
  private static final int DESTINATION_PARTITIONS1 = 8;
  private static final int DESTINATION_PARTITIONS2 = 4;
  public static final String EVENTHUB_KEY_NAME = "RootManageSharedAccessKey";
  public static final String EVENTHUB_KEY = "VJhm+uSYQqDZ1WUN5yeraFiDJg+6BGuNxoqy/L/JVDw=";

  public static final String SYSTEM_NAME = "system1";

  @Test
  public void testSystemFactoryCreateAndStartProducer() {
    Config eventHubConfig = createEventHubConfig();
    EventHubSystemFactory systemFactory = new EventHubSystemFactory();
    SystemProducer systemProducer = systemFactory.getProducer(SYSTEM_NAME, eventHubConfig, new NoOpMetricsRegistry());
    Assert.assertNotNull(systemProducer);

    systemProducer.register(EVENTHUB_NAME1);
    systemProducer.register(EVENTHUB_NAME2);
    systemProducer.start();
    systemProducer.stop();
  }

  private Config createEventHubConfig() {
    return createEventHubConfig(EventHubSystemProducer.PartitioningMethod.EVENT_HUB_HASHING);
  }

  private Config createEventHubConfig(EventHubSystemProducer.PartitioningMethod partitioningMethod) {
    HashMap<String, String> mapConfig = new HashMap<>();
    mapConfig.put(EventHubSystemProducer.CONFIG_PARTITIONING_METHOD, partitioningMethod.toString());
    mapConfig.put(EventHubSystemProducer.CONFIG_EVENT_HUB_NAMESPACE, EVENTHUB_NAMESPACE);
    mapConfig.put(EventHubSystemProducer.CONFIG_EVENT_HUB_SHARED_KEY, EVENTHUB_KEY);
    mapConfig.put(EventHubSystemProducer.CONFIG_EVENT_HUB_SHARED_KEY_NAME, EVENTHUB_KEY_NAME);
    return new MapConfig(mapConfig);
  }

  @Test
  public void testSend() {
    Config eventHubConfig = createEventHubConfig();
    EventHubSystemFactory systemFactory = new EventHubSystemFactory();
    SystemProducer systemProducer = systemFactory.getProducer("system1", eventHubConfig, new NoOpMetricsRegistry());

    systemProducer.register(EVENTHUB_NAME1);

    try {
      systemProducer.send(EVENTHUB_NAME1, createMessageEnvelope(EVENTHUB_NAME1));
      Assert.fail("Sending event before starting producer should throw exception");
    } catch (SamzaException e) {
    }

    systemProducer.start();
    systemProducer.send(EVENTHUB_NAME1, createMessageEnvelope(EVENTHUB_NAME1));

    try {
      systemProducer.send(EVENTHUB_NAME2, createMessageEnvelope(EVENTHUB_NAME1));
      Assert.fail("Sending event to destination that is not registered should throw exception");
    } catch (SamzaException e) {
    }

    try {
      systemProducer.register(EVENTHUB_NAME2);
      Assert.fail("Trying to register after starting producer should throw exception");
    } catch (SamzaException e) {
    }
    systemProducer.stop();
  }

  @Test
  public void testSendToSpecificPartition() {
    Config eventHubConfig = createEventHubConfig(EventHubSystemProducer.PartitioningMethod.PARTITION_KEY_AS_PARTITION);
    EventHubSystemFactory systemFactory = new EventHubSystemFactory();
    SystemProducer systemProducer = systemFactory.getProducer("system1", eventHubConfig, new NoOpMetricsRegistry());

    systemProducer.register(EVENTHUB_NAME1);
    systemProducer.start();
    systemProducer.send(EVENTHUB_NAME1, createMessageEnvelope(EVENTHUB_NAME1, 0));
  }

  private OutgoingMessageEnvelope createMessageEnvelope(String streamName, int partition) {
    return new OutgoingMessageEnvelope(new SystemStream(SYSTEM_NAME, streamName), partition, "key1".getBytes(),
        "value".getBytes());
  }

  private OutgoingMessageEnvelope createMessageEnvelope(String streamName) {
    return new OutgoingMessageEnvelope(new SystemStream(SYSTEM_NAME, streamName), "key1".getBytes(),
        "value".getBytes());
  }

  @Test
  public void testFlush() {
    Config eventHubConfig = createEventHubConfig();
    EventHubSystemFactory systemFactory = new EventHubSystemFactory();
    EventHubSystemProducer systemProducer =
        (EventHubSystemProducer) systemFactory.getProducer("system1", eventHubConfig, new NoOpMetricsRegistry());
    systemProducer.register(EVENTHUB_NAME1);
    systemProducer.register(EVENTHUB_NAME2);
    systemProducer.start();
    int numEvents = 100;
    for (int i = 0; i < numEvents; i++) {
      systemProducer.send(EVENTHUB_NAME1, createMessageEnvelope(EVENTHUB_NAME1));
      systemProducer.send(EVENTHUB_NAME2, createMessageEnvelope(EVENTHUB_NAME2));
    }
    systemProducer.flush(EVENTHUB_NAME1);
    Assert.assertEquals(systemProducer.getPendingFutures().size(), 0);
    systemProducer.stop();
  }
}
