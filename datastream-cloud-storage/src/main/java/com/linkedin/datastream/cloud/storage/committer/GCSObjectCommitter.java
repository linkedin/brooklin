/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage.committer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;


import com.linkedin.datastream.cloud.storage.CommitCallback;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;

/**
 * This is an GCS Object Committer that will commit given files to google cloud storage.
 */
public class GCSObjectCommitter implements ObjectCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(GCSObjectCommitter.class.getName());

    private static final String CONFIG_THREADS = "threads";
    private static final String CONFIG_WRITEATONCE_MAX_FILE_SIZE = "writeAtOnceMaxFileSize";

    private final Storage _storage;
    private final ExecutorService _executor;

    private final int _numOfCommitterThreads;
    private final Meter _uploadRateMeter;
    private final long _writeAtOnceMaxFileSize;

    /**
     * Constructor for GCSObjectCommitter
     * @param properties configuration options for GCSObjectCommitter
     */
    public GCSObjectCommitter(VerifiableProperties properties) {
        String credentialsPath = properties.getString("credentialsPath");
        try {
            Credentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(credentialsPath));
            this._storage = StorageOptions.newBuilder().setCredentials(credentials)
                    .build().getService();
        } catch (FileNotFoundException e) {
            LOG.error("Credentials path {} does not exist", credentialsPath);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOG.error("Unable to read credentials: {}", credentialsPath);
            throw new RuntimeException(e);
        }

        this._numOfCommitterThreads = properties.getInt(CONFIG_THREADS, 1);
        this._writeAtOnceMaxFileSize = properties.getLong(CONFIG_WRITEATONCE_MAX_FILE_SIZE, 1024 * 1024);
        this._executor = Executors.newFixedThreadPool(_numOfCommitterThreads);
        this._uploadRateMeter = DynamicMetricsManager.getInstance().registerMetric(this.getClass().getSimpleName(),
                "uploadRate", Meter.class);
    }

    private static void deleteFile(File file) {
        LOG.info("Deleting file {}", file.toPath());
        if (!file.delete()) {
            LOG.warn("Failed to delete file {}.", file.toPath());
        }

        // clean crc files
        final File crcFile = new File(file.getParent() + "/." + file.getName() + ".crc");
        if (crcFile.exists() && crcFile.isFile()) {
            if (!crcFile.delete()) {
                LOG.warn("Failed to delete crc file {}.", crcFile.toPath());
            }
        }
    }

    private static String getBucketName(final String destination) {
        String[] bucketApplicationDatastream = destination.split("/");
        return bucketApplicationDatastream[0];
    }

    private static String getObjectName(final String destination,
                                        final String topic,
                                        final long partition,
                                        final long startOffset,
                                        final long endOffset,
                                        final String suffix,
                                        final String fileExt) {
        String prefix = destination.substring(destination.indexOf("/") + 1);

        return new StringBuilder()
                .append(prefix)
                .append("/")
                .append(topic)
                .append("/")
                .append(java.time.LocalDate.now())
                .append("/")
                .append(topic)
                .append("+")
                .append(partition)
                .append("+")
                .append(startOffset)
                .append("+")
                .append(endOffset)
                .append("+")
                .append(suffix)
                .append(".")
                .append(fileExt).toString();
    }

    @Override
    public void commit(final String filePath,
                       final String fileFormat,
                       final String destination,
                       final String topic,
                       final long partition,
                       final long minOffset,
                       final long maxOffset,
                       final List<SendCallback> ackCallbacks,
                       final List<DatastreamRecordMetadata> recordMetadata,
                       final CommitCallback callback) {
        final Runnable committerTask = () -> {
            Exception exception = null;
            final File file = new File(filePath);
            final String[] topicPartitionSuffix = file.getName().split("\\+");
            final String objectName = getObjectName(destination,
                    topic,
                    partition,
                    minOffset,
                    maxOffset,
                    topicPartitionSuffix[2],
                    fileFormat);
            try {
                final BlobInfo sourceBlob = BlobInfo
                        .newBuilder(BlobId.of(getBucketName(destination), objectName))
                        .setContentType(Files.probeContentType(file.toPath()))
                        .build();

                LOG.info("Committing Object {}", objectName);

                if (file.getTotalSpace() <= _writeAtOnceMaxFileSize) {
                    Blob blob = _storage.create(sourceBlob, Files.readAllBytes(file.toPath()));
                } else {
                    LOG.info("Using writer channel.");
                    try (WriteChannel writer = _storage.writer(sourceBlob)) {
                        byte[] buffer = new byte[256 * 1024];
                        try (InputStream input = Files.newInputStream(Paths.get(file.getAbsolutePath()))) {
                            int readSize;
                            while ((readSize = input.read(buffer)) >= 0) {
                                writer.write(ByteBuffer.wrap(buffer, 0, readSize));
                            }
                        }
                    }
                }
                _uploadRateMeter.mark(ackCallbacks.size());
            } catch (IOException e) {
                LOG.error("Failed to commit file {} - {}", filePath, e);
                exception = new DatastreamTransientException(e);
            } catch (Exception e) {
                LOG.error("Failed to commit file {} - {}", filePath, e);
                exception = new DatastreamTransientException(e);
            }

            deleteFile(file);

            LOG.info("Successfully created object {}", objectName);
            for (int i = 0; i < ackCallbacks.size(); i++) {
                ackCallbacks.get(i).onCompletion(recordMetadata.get(i), exception);
            }
            callback.commited();
        };
        _executor.execute(committerTask);
    }

    @Override
    public void shutdown() {
        _executor.shutdown();
        try {
            if (!_executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Committer shutdown timed out.");
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while awaiting committer termination.");
            Thread.currentThread().interrupt();
        }
        LOG.info("Object committer stopped.");
    }
}
