/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage.io;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.cloud.storage.Package;
import com.linkedin.datastream.common.VerifiableProperties;
/**
 * Implementation of {@link com.linkedin.datastream.cloud.storage.io.File} to support parquet file format for scribe
 */
public class ScribeParquetFile implements com.linkedin.datastream.cloud.storage.io.File {
    private static final Logger LOG = LoggerFactory.getLogger(AvroParquetFile.class);
    private static final String CONFIG_PAGE_SIZE = "pageSize";
    private static final int DEFAULT_PAGE_SIZE = 64 * 1024;
    private static final CompressionCodecName COMPRESSION_TYPE = CompressionCodecName.SNAPPY;
    private static final String LOGLINE_NAME = "logLine";
    private static final Schema SCHEMA = SchemaBuilder
            .record("rawJson").doc("Raw Tracking Data Json").namespace("com.wayfair.scribe")
            .fields()
            .name(LOGLINE_NAME).type().stringType().noDefault()
            .endRecord();
    private Path _path;
    private ParquetWriter<GenericRecord> _parquetWriter;
    private int _pageSize;
    /**
     * Constructor for ScribeParquetFile
     *
     * @param path path of the file
     * @param props io configuration options
     * @throws IOException
     */
    public ScribeParquetFile(String path, VerifiableProperties props) throws IOException {
        this._path = new Path(path);
        this._parquetWriter = null;
        this._pageSize = props.getInt(CONFIG_PAGE_SIZE, DEFAULT_PAGE_SIZE);
    }
    @Override
    public long length() {
        return this._parquetWriter.getDataSize();
    }
    @Override
    public void write(Package aPackage) throws IOException {
        if (_parquetWriter == null) {
            _parquetWriter = AvroParquetWriter.<GenericRecord>builder(_path)
                    .withSchema(SCHEMA)
                    .withCompressionCodec(COMPRESSION_TYPE)
                    .withPageSize(_pageSize)
                    .build();
        }
        final GenericRecord record = new GenericData.Record(SCHEMA);
        record.put(LOGLINE_NAME, new String((byte[]) aPackage.getRecord().getValue(), StandardCharsets.UTF_8));
        _parquetWriter.write(record);
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