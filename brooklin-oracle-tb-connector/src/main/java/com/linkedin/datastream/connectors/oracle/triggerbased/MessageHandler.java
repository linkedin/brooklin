package com.linkedin.datastream.connectors.oracle.triggerbased;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.databases.DatabaseColumnRecord;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.AvroMessageEncoderUtil;
import com.linkedin.datastream.common.AvroEncodingException;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.connectors.oracle.triggerbased.consumer.OracleChangeEvent;


/**
 * Each DatastreamTask has its own Message Handler. The OracleTaskHandler will pass a list of
 * OracleChangeEvents from a specific source to the MessageHandler. This class then
 * reads through the events and creates the BrooklinEnvelope. The BrooklinEnvelope
 * then gets passed to the Transport Provider
 *
 * Upon receiving a OracleChangeEvent that contains Records representing changed row Data,
 * the MessageHandler runs on tight loop where it reads the events one by one, each time,
 * converting the data to a BrooklinEnvelope and writing it to the Transport Provider.
 * This is very similar to processing a Snapshot in other Connectors
 */
public class MessageHandler implements SendCallback {
  private static final Logger LOG = LoggerFactory.getLogger(MessageHandler.class);
  private static final String DEFAULT_PRIMARY_KEY_FIELD = "key";

  private final OracleSource _source;
  private final Schema _schema;
  private final String _schemaId;

  private boolean _isErrorState = false;
  private DatastreamTask _task;
  private String _primaryKeyField;

  public MessageHandler(OracleSource source,
      Schema schema,
      DatastreamTask task) {

    _source = source;
    _task = task;

    _schema = schema;
    _schemaId = SchemaUtil.getSchemaId(task);
    _primaryKeyField = getPrimaryKeyFieldFromSchema(_schema);
  }

  public void processChangeEvent(OracleChangeEvent event) throws DatastreamException {
    BrooklinEnvelope brooklinEnvelope;

    try {
      brooklinEnvelope = generateBrooklinEnvelope(event);
    } catch (DatastreamException e) {
      LOG.error("Failed generating DatastreamEvent", e);
      throw e;
    }

    sendDatastreamEvent(brooklinEnvelope);
  }

  public Boolean isErrorState() {
    return _isErrorState;
  }

  /**
   * grab the primary key field from the Avro Schema. When Schema's are generated
   * This connector requires the primary key be identified as `pk` in the metadata field.
   * If schema generation were to change in the future, this function would need to be
   * updated as well.
   *
   * @param schema - The Avro Schema representing the Oracle Schema
   * @return the field used as the primary key
   * @throws com.linkedin.datastream.common.DatastreamRuntimeException if pk not found
   */
  public static String getPrimaryKeyFieldFromSchema(Schema schema) {
    String meta = schema.getProp("meta");
    String primaryKey = null;

    if (meta == null) {
      throw new DatastreamRuntimeException(String.format("Could not found metadata property in Schema: %s", schema.getFullName()));
    }

    for (String declaration : meta.split(";")) {
      String[] equation = declaration.split("=");
      if (equation[0].equals("pk")) {
        primaryKey = equation[1];
        break;
      }
    }

    if (primaryKey != null) {
      LOG.info(String.format("found primary key for schema: %s", primaryKey));
      return primaryKey;
    }

    LOG.warn(
        String.format("Could not find pk metadata from Schema: %s using 'key' as primaryKey", schema.getFullName()));
    if (schema.getField(DEFAULT_PRIMARY_KEY_FIELD) == null) {
      throw new DatastreamRuntimeException(
          String.format("Could not found pk metadata or field named 'key' in Schema: %s", schema.getFullName()));
    }

    return DEFAULT_PRIMARY_KEY_FIELD;
  }

  /**
   * Reset the Error state
   */
  public void resetErrorState() {
    _isErrorState = false;
  }

  /**
   * Callback method for the Async call to the TransportProvider.
   * If there is an error in sending the DatastreamEvent, we need to set
   * {@code _isErrorState} to TRUE. This will be later used by the OracleTaskHandler
   * to reprocess from a safe checkpoint
   *
   * @param metadata
   * @param exception
   */
  @Override
  public void onCompletion(DatastreamRecordMetadata metadata, Exception exception) {
    // do nothing if there is no exception
    if (exception == null) {
      return;
    }

    synchronized (this) {
      _isErrorState = true;
      LOG.error("Failed to send datastream record: {}", metadata, exception);
    }
  }

  BrooklinEnvelope generateBrooklinEnvelope(OracleChangeEvent event) throws DatastreamException {

    byte[] payload = constructDatastreamEventPayload(event);
    byte[] key = constructDatastreamEventKey(event);
    Map<String, String> metadata = constructDatstreamEventMetadata(event);

    return new BrooklinEnvelope(key, payload, null, metadata);
  }

  private byte[] constructDatastreamEventPayload(OracleChangeEvent event) throws DatastreamException {
    GenericRecord payloadRecord = new GenericData.Record(_schema);

    try {
      for (DatabaseColumnRecord record : event.getRecords()) {
        try {
          payloadRecord.put(record.getColName(), record.getValue());
        } catch (Exception e) {
          LOG.error("Failed to put colName: {} => Val: {} into schema: {}", record.getColName(), record.getValue(), _schema.getName());
          throw e;
        }

      }
    } catch (Exception e) {
      LOG.error("Failed to generate payload from DatastreamEvent with schemaId: {}", _schemaId, e);
      throw new DatastreamException(e);
    }


    try {
      return AvroMessageEncoderUtil.encode(_schemaId, payloadRecord);
    } catch (AvroEncodingException e) {
      LOG.error("Failed to encode datastreamEvent payload from schemaId: {}", _schemaId, e);
      throw new DatastreamException(e);
    }
  }

  private byte[] constructDatastreamEventKey(OracleChangeEvent event) throws DatastreamException {

    for (DatabaseColumnRecord record : event.getRecords()) {
      if (!_primaryKeyField.equals(record.getColName())) {
        continue;
      }

      if (record.getValue() instanceof Long) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong((Long) record.getValue());
        return buffer.array();
      }

      if (record.getValue() instanceof String) {
        String key = (String) record.getValue();
        return key.getBytes();
      }

      throw new DatastreamException(
          String.format("value: %s is not of instance String or Long", record.getValue().getClass()));
    }

    // if we reached this point, that means we were not able to find the primaryKey field in the
    // ResultSet. (If this happens this is most likely means that the Schema is not up to date with
    // the actual Oracle table/view)
    throw new DatastreamRuntimeException(
        String.format("primaryKey: %s could not be found in Schema: %s ", _primaryKeyField, _schema.getFullName()));
  }

  private Map<String, String> constructDatstreamEventMetadata(OracleChangeEvent event) {
    Map<String, String> metadata = new HashMap<>();

    metadata.put(BrooklinEnvelopeMetadataConstants.OPCODE, BrooklinEnvelopeMetadataConstants.OpCode.UPDATE.toString());
    metadata.put(BrooklinEnvelopeMetadataConstants.SOURCE_TIMESTAMP, String.valueOf(event.getSourceTimestamp()));
    metadata.put(BrooklinEnvelopeMetadataConstants.DATABASE, _source.getDbName());
    metadata.put(BrooklinEnvelopeMetadataConstants.TABLE, _source.getViewName());
    metadata.put(BrooklinEnvelopeMetadataConstants.SCN, String.valueOf(event.getScn()));

    return metadata;
  }

  /**
   * Send the datastreamEvent into the respective Transport Provider.
   * @param brooklinEnvelope
   */
  private void sendDatastreamEvent(BrooklinEnvelope brooklinEnvelope) {
    DatastreamProducerRecord producerRecord = buildProducerRecord(brooklinEnvelope);
    _task.getEventProducer().send(producerRecord, this);
  }

  private DatastreamProducerRecord buildProducerRecord(BrooklinEnvelope brooklinEnvelope) {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.setPartitionKey(new String((byte[]) brooklinEnvelope.key().get()));
    builder.addEvent(brooklinEnvelope);

    long sourceTimestamp =
        Long.parseLong(brooklinEnvelope.getMetadata().get(BrooklinEnvelopeMetadataConstants.SOURCE_TIMESTAMP));
    String checkpoint = brooklinEnvelope.getMetadata().get(BrooklinEnvelopeMetadataConstants.SCN);

    builder.setEventsSourceTimestamp(sourceTimestamp);
    builder.setSourceCheckpoint(checkpoint);

    return builder.build();
  }
}