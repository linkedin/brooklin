package com.linkedin.brooklin.samza;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;


public class SamzaTransportProvider implements TransportProvider {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaTransportProvider.class);

  public static final String CONFIG_FACTORY_CLASS_NAME = "systemFactoryClassName";
  public static final String CONFIG_SERDE_FACTORY_CLASS_NAME = "serdeFactoryClassName";
  public static final String CONFIG_SERDE_NAME = "serdeName";
  public static final String CONFIG_PREFIX_KEY_SERDE = "key";
  public static final String CONFIG_PREFIX_MSG_SERDE = "msg";

  public static final String CONFIG_DESTINATION_NUM_PARTITION = "destinationPartitions";

  private final MapConfig _samzaConfig;
  private final String _factoryClassName;

  private SystemProducer _systemProducer;
  private final String _systemName;
  private final String _destination;
  private final SystemStream _systemStream;
  private final Serde<Object> _keySerde;
  private final Serde<Object> _msgSerde;
  private List<SendCallback> _callbacks = new ArrayList<>();

  public SamzaTransportProvider(Properties transportProviderProperties, Datastream datastream) {
    VerifiableProperties properties = new VerifiableProperties(transportProviderProperties);
    _factoryClassName = properties.getString(CONFIG_FACTORY_CLASS_NAME);

    _keySerde = getSerde(properties, CONFIG_PREFIX_KEY_SERDE);
    _msgSerde = getSerde(properties, CONFIG_PREFIX_MSG_SERDE);

    _systemName = datastream.getName();

    _samzaConfig = mergeAndGetSamzaSystemConfig(transportProviderProperties, datastream);

    _destination = datastream.getDestination().getConnectionString();
    _systemStream = new SystemStream(_systemName, _destination);
    _systemProducer = createAndInitializeSystemProducer();
  }

  private SystemProducer createAndInitializeSystemProducer() {
    SystemFactory samzaFactory = ReflectionUtils.createInstance(_factoryClassName);
    if (samzaFactory == null) {
      String msg = "Unable to instantiate the factory class " + _factoryClassName;
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    SystemProducer systemProducer = samzaFactory.getProducer(_systemName, _samzaConfig, new NoOpMetricsRegistry());
    systemProducer.register(_destination);
    systemProducer.start();
    return systemProducer;
  }

  private Serde<Object> getSerde(VerifiableProperties props, String configPrefixKeySerde) {
    Serde<Object> serde = null;
    Properties keySerdeProps = props.getDomainProperties(configPrefixKeySerde);
    String serdeFactoryClassName = keySerdeProps.getProperty(CONFIG_SERDE_FACTORY_CLASS_NAME);
    if (StringUtils.isNotEmpty(serdeFactoryClassName)) {
      SerdeFactory<Object> serdeFactory = ReflectionUtils.createInstance(serdeFactoryClassName);
      if (serdeFactory == null) {
        String msg = "Unable to instantiate the factory class " + serdeFactoryClassName;
        LOG.warn(msg);
        return null;
      }

      String serdeName = keySerdeProps.getProperty(CONFIG_SERDE_NAME);
      if (StringUtils.isEmpty(serdeName)) {
        String msg = "Serde name is empty";
        LOG.warn(msg);
        return null;
      }

      Properties serdeConfig = props.getDomainProperties(serdeName);
      Config samzaConfig = new MapConfig(convertToMapConfig(serdeConfig));
      serde = serdeFactory.getSerde(serdeName, samzaConfig);
    }

    return serde;
  }

  SystemProducer getSystemProducer() {
    return _systemProducer;
  }

  protected MapConfig mergeAndGetSamzaSystemConfig(Properties transportProviderProperties, Datastream datastream) {
    HashMap<String, String> samzaConfig = convertToMapConfig(transportProviderProperties);

    StringMap datastreamMetadata = datastream.getMetadata();
    String systemPrefix = datastream.getTransportProviderName() + ".";
    datastreamMetadata.keySet()
        .stream()
        .filter(key -> key.startsWith(systemPrefix))
        .forEach(key -> samzaConfig.put(key.replace(systemPrefix, ""), datastreamMetadata.get(key)));
    samzaConfig.put(CONFIG_DESTINATION_NUM_PARTITION, datastream.getDestination().getPartitions().toString());
    return new MapConfig(samzaConfig);
  }

  private HashMap<String, String> convertToMapConfig(Properties properties) {
    HashMap<String, String> samzaConfig = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      samzaConfig.put(key, properties.getProperty(key));
    }

    return samzaConfig;
  }

  @Override
  public synchronized void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {

    if (_systemProducer == null) {
      _systemProducer = createAndInitializeSystemProducer();
    }

    if (!destination.equals(_destination)) {
      String msg =
          String.format("Trying to send an event to invalid destination %s, supported destination %s", destination,
              _destination);
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    if (!record.getPartition().isPresent()) {
      String msg = "Record doesn't contain the partition to which it needs to be sent.";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    int partition = record.getPartition().get();

    List<OutgoingMessageEnvelope> messages = convertToOutgoingMessageEnvelope(record, partition);
    for (OutgoingMessageEnvelope message : messages) {
      try {
        _systemProducer.send(destination, message);
      } catch (Exception e) {
        String msg = "send failed with exception. Resetting the system producer.";
        _callbacks.clear();
        _systemProducer = null;
        LOG.error(msg, e);
        throw new DatastreamRuntimeException(msg, e);
      }
    }

    DatastreamRecordMetadata metadata =
        new DatastreamRecordMetadata(record.getCheckpoint(), _systemStream.getStream(), partition);

    if (onComplete != null) {
      _callbacks.add((m, exception) -> onComplete.onCompletion(metadata, exception));
    }
  }

  private List<OutgoingMessageEnvelope> convertToOutgoingMessageEnvelope(DatastreamProducerRecord record,
      int partition) {
    return record.getEvents()
        .stream()
        .map(event -> convertToOutgoingMessageEnvelope(partition, event.getKey(), event.getValue()))
        .collect(Collectors.toList());
  }

  private OutgoingMessageEnvelope convertToOutgoingMessageEnvelope(int partition, Object key, Object value) {
    if (_keySerde != null) {
      key = _keySerde.toBytes(key);
    }

    if (_msgSerde != null) {
      value = _msgSerde.toBytes(value);
    }

    return new OutgoingMessageEnvelope(_systemStream, partition, key, value);
  }

  @Override
  public void close() {
    if (_systemProducer != null) {
      _systemProducer.stop();
    }
  }

  @Override
  public synchronized void flush() {

    // If the system producer is null, It means that the previous sends failed. There is nothing to flush.
    if (_systemProducer == null) {
      return;
    }

    try {
      _systemProducer.flush(_destination);

      // Call the callbacks only when the flush succeeds. If the flush fails. Then the expectation is that
      // the connector has to revert back to old safe checkpoint and retry.
      for (SendCallback onComplete : _callbacks) {
        onComplete.onCompletion(null, null);
      }
      _callbacks.clear();
    } catch (RuntimeException e) {
      String msg = "flush failed with exception. Resetting the system producer.";
      _callbacks.clear();
      _systemProducer = null;
      throw new DatastreamRuntimeException(msg, e);
    }
  }
}
