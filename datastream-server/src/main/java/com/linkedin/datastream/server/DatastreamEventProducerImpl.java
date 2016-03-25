package com.linkedin.datastream.server;

import java.util.Objects;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryException;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProvider;

/**
 * Implementation of the DatastremaEventProducer that connector will use to produce events. There is an unique
 * DatastreamEventProducerImpl object created per DatastreamTask that is assigned to the connector.
 * DatastreamEventProducer will inturn use a shared EventProducer (shared across the tasks that use the same destinations)
 * to produce the events.
 */
public class DatastreamEventProducerImpl implements DatastreamEventProducer {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamEventProducerImpl.class);

  private final SchemaRegistryProvider _schemaRegistryProvider;
  private final EventProducer _eventProducer;
  private final DatastreamTask _task;

  public DatastreamEventProducerImpl(DatastreamTask task, SchemaRegistryProvider schemaRegistryProvider,
      EventProducer eventProducer) {
    _schemaRegistryProvider = schemaRegistryProvider;
    _eventProducer = eventProducer;
    _task = task;
  }

  EventProducer getEventProducer() {
    return _eventProducer;
  }

  @Override
  public void send(DatastreamProducerRecord event) {
    _eventProducer.send(_task, event);
  }

  /**
   * Register the schema in schema registry. If the schema already exists in the registry
   * Just return the schema Id of the existing
   * @param schemaName Name of the schema. Schema within the same name needs to be backward compatible.
   * @param schema Schema that needs to be registered.
   * @return
   *   SchemaId of the registered schema.
   */
  @Override
  public String registerSchema(String schemaName, Schema schema) throws SchemaRegistryException {
    if (_schemaRegistryProvider != null) {
      return _schemaRegistryProvider.registerSchema(schemaName, schema);
    } else {
      String errorMessage = "SchemaRegistryProvider is not configured, So registerSchema is not supported";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    return null;
  }

  @Override
  public void flush() {
    _eventProducer.flush();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatastreamEventProducerImpl producer = (DatastreamEventProducerImpl) o;
    return Objects.equals(_schemaRegistryProvider, producer._schemaRegistryProvider) &&
            Objects.equals(_eventProducer, producer._eventProducer) &&
            Objects.equals(_task, producer._task);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_schemaRegistryProvider, _eventProducer, _task);
  }
}
