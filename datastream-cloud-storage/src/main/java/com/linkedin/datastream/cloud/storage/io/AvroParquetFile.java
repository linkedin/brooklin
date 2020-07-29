/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage.io;

import java.io.IOException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.VerifiableProperties;

/**
 * Implementation of {@link com.linkedin.datastream.cloud.storage.io.File} to support Parquet file format
 */
public class AvroParquetFile implements com.linkedin.datastream.cloud.storage.io.File {
    private static final Logger LOG = LoggerFactory.getLogger(AvroParquetFile.class);

    private final static Map<String, Schema> SCHEMAS = new ConcurrentHashMap<>();

    private static final String CONFIG_SCHEMA_REGISTRY_URL = "schemaRegistryURL";
    private static final String CONFIG_SCHEMA_NAME_SUFFIX = "schemaNameSuffix";
    private static final String CONFIG_PAGE_SIZE = "pageSize";

    private static final String DEFAULT_CONFLUENT_SCHEMA_NAME_SUFFIX = "-value";
    private static final int DEFAULT_PAGE_SIZE = 64 * 1024;

    private static final CompressionCodecName COMPRESSION_TYPE = CompressionCodecName.SNAPPY;

    private Path _path;
    private String _schemaRegistryURL;
    private ParquetWriter<GenericRecord> _parquetWriter;

    private KafkaAvroDeserializer _deserializer;
    private String _schemaNameSuffix;
    private SchemaRegistryClient _schemaRegistryClient;
    private int _pageSize;

    /**
     * Constructor for AvroParquetFile
     *
     * @param path path of the file
     * @param props io configuration options
     * @throws IOException
     */
    public AvroParquetFile(String path, VerifiableProperties props) throws IOException {
        this._path = new Path(path);
        this._parquetWriter = null;
        this._schemaRegistryURL = props.getString(CONFIG_SCHEMA_REGISTRY_URL);
        this._schemaRegistryClient = new CachedSchemaRegistryClient(_schemaRegistryURL, Integer.MAX_VALUE);
        this._schemaNameSuffix = props.getString(CONFIG_SCHEMA_NAME_SUFFIX, DEFAULT_CONFLUENT_SCHEMA_NAME_SUFFIX);
        this._pageSize = props.getInt(CONFIG_PAGE_SIZE, DEFAULT_PAGE_SIZE);
        this._deserializer = new KafkaAvroDeserializer(_schemaRegistryClient);
    }

    private Schema getSchemaByTopic(String topic) {
        String key = _schemaRegistryURL + "-" + topic;
        Schema schema =  SCHEMAS.computeIfAbsent(key, (k) -> {
            try {
                String schemaName = topic + _schemaNameSuffix;
                return new Schema.Parser().parse(_schemaRegistryClient.getLatestSchemaMetadata(schemaName).getSchema());
            } catch (Exception e) {
                LOG.error("Unable to find schema for {} - {}", key, e);
                return null;
            }
        });

        if (schema == null) {
            throw new IllegalStateException("Avro schema not found for topic " + topic);
        }
        return schema;
    }

    @Override
    public long length() {
        return this._parquetWriter.getDataSize();
    }

    @Override
    public void write(Package aPackage) throws IOException {
        if (_parquetWriter == null) {
            _parquetWriter = AvroParquetWriter.<GenericRecord>builder(_path)
                    .withSchema(getSchemaByTopic(aPackage.getTopic()))
                    .withCompressionCodec(COMPRESSION_TYPE)
                    .withPageSize(_pageSize)
                    .build();
        }
        _parquetWriter.write((GenericRecord) _deserializer.deserialize(
                aPackage.getTopic(), (byte[]) aPackage.getRecord().getValue()));
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