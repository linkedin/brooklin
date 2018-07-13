package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;

import static org.mockito.Mockito.*;


public class TestAbstractKafkaConnector {

  private static final Logger LOG = LoggerFactory.getLogger(TestAbstractKafkaConnector.class);

  @Test
  public void testConnectorRestartCalled() {
    Properties props = new Properties();
    props.setProperty("daemonThreadIntervalInSeconds", "1");
    TestKafkaConnector connector = new TestKafkaConnector(false, props);
    DatastreamTask datastreamTask = new DatastreamTaskImpl();
    connector.onAssignmentChange(Collections.singletonList(datastreamTask));
    connector.start(null);
    PollUtils.poll(() -> connector.getCreateTaskCalled() >= 3, Duration.ofSeconds(1).toMillis(),
        Duration.ofSeconds(10).toMillis());
    Assert.assertTrue(connector.getCreateTaskCalled() >= 3);

    connector.stop();
  }

  @Test
  public void testRestartThrowsException() {
    Properties props = new Properties();
    props.setProperty("daemonThreadIntervalInSeconds", "1");
    TestKafkaConnector connector = new TestKafkaConnector(true, props);
    DatastreamTask datastreamTask = new DatastreamTaskImpl();
    connector.onAssignmentChange(Collections.singletonList(datastreamTask));
    connector.start(null);
    PollUtils.poll(() -> connector.getCreateTaskCalled() >= 3, Duration.ofSeconds(1).toMillis(),
        Duration.ofSeconds(10).toMillis());
    Assert.assertTrue(connector.getCreateTaskCalled() >= 3);

    connector.stop();
  }

  public class TestKafkaConnector extends AbstractKafkaConnector {

    private boolean _restartThrows;
    private int _createTaskCalled = 0;

    public TestKafkaConnector(boolean restartThrows, Properties props) {
      super("test", props, new KafkaGroupIdConstructor(
          Boolean.parseBoolean(props.getProperty(IS_GROUP_ID_HASHING_ENABLED, Boolean.FALSE.toString())),
          "TestkafkaConnectorCluster"), "TestkafkaConnectorCluster", LOG);
      _restartThrows = restartThrows;
    }

    @Override
    protected AbstractKafkaBasedConnectorTask createKafkaBasedConnectorTask(DatastreamTask task) {
      _createTaskCalled++;
      AbstractKafkaBasedConnectorTask connectorTask = mock(AbstractKafkaBasedConnectorTask.class);
      try {
        when(connectorTask.awaitStop(anyLong(), anyObject())).thenReturn(true);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return connectorTask;
    }

    @Override
    public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
        throws DatastreamValidationException {
    }

    @Override
    protected boolean isTaskRunning(DatastreamTask datastreamTask) {
      return false;
    }

    @Override
    protected void restartIfNotRunning(DatastreamTask task) {
      if (_restartThrows) {
        _restartThrows = false;
        throw new RuntimeException();
      }
      super.restartIfNotRunning(task);
    }

    public int getCreateTaskCalled() {
      return _createTaskCalled;
    }
  }
}
