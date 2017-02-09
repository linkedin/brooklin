package com.linkedin.datastream.connectors.kafka;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class KafkaConnectorFactory implements ConnectorFactory<KafkaConnector> {

  @Override
  public KafkaConnector createConnector(String connectorName, Properties config) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(config);
    long commitMillis = verifiableProperties.getLongInRange(
        KafkaConnector.COMMIT_INTERVAL_MILLIS,
        TimeUnit.SECONDS.toMillis(30),
        0,
        TimeUnit.HOURS.toMillis(1)
    );
    return new KafkaConnector(connectorName, commitMillis);
  }
}
