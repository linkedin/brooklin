/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.Writer;

import com.linkedin.datastream.cloud.storage.Package;
import com.linkedin.datastream.common.VerifiableProperties;

/**
 * Implementation of {@link com.linkedin.datastream.cloud.storage.io.File} to support SequenceFile file format
 */
public class SequenceFile implements File {
    private final org.apache.hadoop.io.SequenceFile.Writer _writer;
    private final Path _path;
    private final LongWritable _keyLong = new LongWritable();
    private final BytesWritable _valueBytes = new BytesWritable();

    /**
     * Constructor for SequenceFile
     * @param path path of the file
     * @param properties io configuration options
     * @throws IOException
     */
    public SequenceFile(String path, VerifiableProperties properties) throws IOException {
        _path = new Path(path);
        this._writer = org.apache.hadoop.io.SequenceFile.createWriter(
                new Configuration(),
                Writer.file(_path),
                Writer.keyClass(_keyLong.getClass()),
                Writer.valueClass(_valueBytes.getClass()));
    }

    @Override
    public long length() throws IOException {
        return this._writer.getLength();
    }

    @Override
    public void write(Package aPackage) throws IOException {
        this._keyLong.set(aPackage.getOffset());
        byte[] value = (byte[]) aPackage.getRecord().getValue();
        this._valueBytes.set(value, 0, value.length);
        this._writer.append(_keyLong, _valueBytes);
    }

    @Override
    public void close() throws IOException {
        this._writer.close();
    }

    @Override
    public String getPath() {
        return _path.toString();
    }

    @Override
    public String getFileFormat() {
        return "sequence";
    }

}
