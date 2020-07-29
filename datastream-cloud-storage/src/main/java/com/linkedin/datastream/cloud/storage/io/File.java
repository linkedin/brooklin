/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage.io;

import java.io.IOException;

import com.linkedin.datastream.common.Package;

/**
 * File interface that should be implemented to support different file formats. Examples, SequenceFile, Parquet
 */
public interface File {
    /**
     * returns the length of the file
     * @return length of the file
     * @throws IOException
     */
    long length() throws IOException;

    /**
     * writes
     * @param aPackage writes the record in the package to the file
     * @throws IOException
     */
    void write(Package aPackage) throws IOException;

    /**
     * closes the file
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * returns the path of the file
     * @return path of the file
     */
    String getPath();

    /**
     * returns the  of the file format
     * @return format/extension of the file
     */
    String getFileFormat();
}
