package com.linkedin.brooklin.samza;

import java.util.Properties;

import org.apache.samza.config.Config;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.Test;

import junit.framework.Assert;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.testutil.DatastreamTestUtils;


public class TestSamzaTransportProviderAdmin {

  public static final String DATASTREAM_NAME = "ds1";
  public static final int NUM_PARTITION = 1;
  public static final String DESTINATION = "destination";
  public static final String TRANSPORT_PROVIDER_NAME = "tpName";

  @Test
  public void testAssignTask() {
    SamzaTransportProviderAdmin admin = createSamzaTransportProviderAdmin();
    DatastreamTask task = createDatastreamTask();
    TransportProvider tp = admin.assignTransportProvider(task);
    TransportProvider tp2 = admin.assignTransportProvider(task);
    Assert.assertEquals(tp, tp2);
    Assert.assertTrue(tp instanceof SamzaTransportProvider);
    SamzaTransportProvider samzatp = (SamzaTransportProvider) tp;
    SystemProducer sp = samzatp.getSystemProducer();
    Assert.assertTrue(sp instanceof MockSystemFactory.MockSystemProducer);
    MockSystemFactory.MockSystemProducer msp = (MockSystemFactory.MockSystemProducer) sp;
    Assert.assertTrue(msp.isStarted());
    Config config = msp.getConfig();
    Assert.assertEquals(DATASTREAM_NAME, msp.getSystemName());
    Assert.assertEquals(msp.getSources().size(), 1);
    Assert.assertEquals(msp.getSources().get(0), DESTINATION);
    Assert.assertEquals(config.getInt(SamzaTransportProvider.CONFIG_DESTINATION_NUM_PARTITION), NUM_PARTITION);
    Assert.assertEquals(config.get(SamzaTransportProvider.CONFIG_FACTORY_CLASS_NAME),
        MockSystemFactory.class.getName());
  }

  @Test
  public void testUnAssignTask() {
    SamzaTransportProviderAdmin admin = createSamzaTransportProviderAdmin();
    DatastreamTask task = createDatastreamTask();
    SamzaTransportProvider tp = (SamzaTransportProvider) admin.assignTransportProvider(task);
    MockSystemFactory.MockSystemProducer sp = (MockSystemFactory.MockSystemProducer) tp.getSystemProducer();
    admin.unassignTransportProvider(task);
    Assert.assertTrue(sp.isStopped());
    SamzaTransportProvider tp2 = (SamzaTransportProvider) admin.assignTransportProvider(task);
    Assert.assertNotSame(tp, tp2);
    sp = (MockSystemFactory.MockSystemProducer) tp.getSystemProducer();
    Assert.assertTrue(sp.isStarted());
  }

  @Test
  public void testInitializeDestinationForDatastream() throws DatastreamValidationException {
    SamzaTransportProviderAdmin admin = createSamzaTransportProviderAdmin();
    Datastream ds = createDatastream();
    try {
      admin.initializeDestinationForDatastream(ds);
      Assert.fail();
    } catch (DatastreamValidationException e) {
    }

    ds.setTransportProviderName("WRONG" + TRANSPORT_PROVIDER_NAME);

    try {
      admin.initializeDestinationForDatastream(ds);
      Assert.fail();
    } catch (DatastreamValidationException e) {
    }

    ds.setTransportProviderName(TRANSPORT_PROVIDER_NAME);
    admin.initializeDestinationForDatastream(ds);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testGetRetentionThrowsUnsupportedOperation() {
    SamzaTransportProviderAdmin admin = createSamzaTransportProviderAdmin();
    Datastream ds = createDatastream();
    admin.getRetention(ds);
  }

  public static SamzaTransportProviderAdmin createSamzaTransportProviderAdmin() {
    Properties config = createSamzaTransportProviderConfigs();
    return new SamzaTransportProviderAdmin(TRANSPORT_PROVIDER_NAME, config);
  }

  public static Properties createSamzaTransportProviderConfigs() {
    Properties props = new Properties();
    props.put(SamzaTransportProvider.CONFIG_FACTORY_CLASS_NAME, MockSystemFactory.class.getName());
    return props;
  }

  public static DatastreamTask createDatastreamTask() {
    Datastream ds = createDatastream();
    return new DatastreamTaskImpl(ds);
  }

  public static Datastream createDatastream() {
    return DatastreamTestUtils.createDatastream("test", DATASTREAM_NAME, "source", DESTINATION, NUM_PARTITION);
  }
}
