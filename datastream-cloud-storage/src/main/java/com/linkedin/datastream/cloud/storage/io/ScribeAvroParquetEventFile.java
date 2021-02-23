/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage.io;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.VerifiableProperties;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScribeAvroParquetEventFile class writes out scribe 2.0 event data from Kafka topics which is in avro format to Parquet files into GCS
 * buckets using Brooklin Connectors.
 */
public class ScribeAvroParquetEventFile implements File {
    private static final Logger LOG = LoggerFactory.getLogger(ScribeAvroParquetEventFile.class);

    private final static Map<String, Schema> SCHEMAS = new ConcurrentHashMap<>();

    private static final String CONFIG_SCHEMA_REGISTRY_URL = "schemaRegistryURL";

    private static final String CONFIG_PAGE_SIZE = "pageSize";

    // scribe 2.0 subject prefix
    private static final String DEFAULT_SCRIBE_CONFLUENT_SCHEMA_NAME_PREFIX = "scribe.v2.events.";

    private static final int DEFAULT_PAGE_SIZE = 64 * 1024;

    private static final CompressionCodecName COMPRESSION_TYPE = CompressionCodecName.SNAPPY;

    private Path _path;
    private String _schemaRegistryURL;
    private ParquetWriter<GenericRecord> _parquetWriter;

    private KafkaAvroDeserializer _deserializer;

    private SchemaRegistryClient _schemaRegistryClient;
    private int _pageSize;
    private Schema scribeParquetSchema;

  /**
   * Constructor for AvroParquetFile
   *
   * @param path path of the file
   * @param props io configuration options
   * @throws IOException
   */
    public ScribeAvroParquetEventFile(String path, VerifiableProperties props) throws IOException {
        this._path = new Path(path);
        this._parquetWriter = null;
        this._schemaRegistryURL = props.getString(CONFIG_SCHEMA_REGISTRY_URL);
        this._schemaRegistryClient = new CachedSchemaRegistryClient(_schemaRegistryURL, Integer.MAX_VALUE);
        this._pageSize = props.getInt(CONFIG_PAGE_SIZE, DEFAULT_PAGE_SIZE);
        this._deserializer = new KafkaAvroDeserializer(_schemaRegistryClient);
    }

  /**
   * Get Schema by Kafka topic name
   *
   * Scribe 2.0 Kafka topic name format:            domain_name-event_name
   * Scribe 2.0 SchemaRegistry subject name format: scribe.v2.events.domain_name.event_name
   *
   * Eg: Kafka topic: supply_chain-item_return_routes_requested, corresponding subject name: scribe.v2.events.supply_chain.item_return_routes_requested
   *
   * @param topic kafka topic
   * @return
   */
    private Schema getSchemaByTopic(String topic) {
        String schemaName = DEFAULT_SCRIBE_CONFLUENT_SCHEMA_NAME_PREFIX + topic.replace("-",".") ;
        Schema schema =  SCHEMAS.computeIfAbsent(schemaName, (k) -> {
          try {
            return new Schema.Parser().parse(_schemaRegistryClient.getLatestSchemaMetadata(schemaName).getSchema());
          } catch (Exception e) {
            LOG.error("Unable to find schema for {} - {}", schemaName, e);
            return null;
          }
        });

        if (schema == null) {
            throw new IllegalStateException("Avro schema not found for topic " + topic);
        }

        try {
          // Convert Avro schema into Scribe 2.0 expected Parquet schema.
          scribeParquetSchema =  ScribeParquetAvroConverter.generateParquetStructuredAvroSchema(schema);
          return scribeParquetSchema;
        } catch (Exception e) {
          LOG.error(String.format("Exception in converting avro schema to parquet in ScribeAvroParquetEventFile: topic: %s, exception: %s", topic, e));
          return null;
        }
    }

    @Override
    public long length() {
        return this._parquetWriter.getDataSize();
    }

    @Override
    public void write(Package aPackage) throws IOException {
      try {
        if (_parquetWriter == null) {
          _parquetWriter = AvroParquetWriter.<GenericRecord>builder(_path)
              .withSchema(getSchemaByTopic(aPackage.getTopic()))
              .withCompressionCodec(COMPRESSION_TYPE)
              .withPageSize(_pageSize)
              .build();
        }
        GenericRecord deserializedAvroGenericRecord = (GenericRecord) _deserializer.deserialize(
            aPackage.getTopic(), (byte[]) aPackage.getRecord().getValue());
        // Transform the data into Scribe 2.0 expected format.
        GenericRecord avroParquetRecord = ScribeParquetAvroConverter.generateParquetStructuredAvroData(
            scribeParquetSchema, deserializedAvroGenericRecord);
        _parquetWriter.write(avroParquetRecord);
      } catch (Exception e) {
        LOG.error(String.format("Exception in converting avro record to parquet record in ScribeAvroParquetEventFile: topic: %s, exception: %s", aPackage.getTopic(), e));
      }
    }

    @Override
    public void close() throws IOException {
        this._parquetWriter.close();
        _parquetWriter = null;
    }

    @Override
    public String getPath() {
        return _path.toString();
    }

    @Override
    public String getFileFormat() {
        return "parquet";
    }
}