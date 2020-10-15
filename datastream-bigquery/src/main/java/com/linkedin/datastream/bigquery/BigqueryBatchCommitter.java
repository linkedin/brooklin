/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.transport.buffered.BatchCommitter;
import com.linkedin.datastream.server.api.transport.buffered.CommitCallback;

/**
 * This class commits submitted batches to BQ tables.
 */
public class BigqueryBatchCommitter implements BatchCommitter<List<InsertAllRequest.RowToInsert>> {
    private static final Logger LOG = LoggerFactory.getLogger(BigqueryBatchCommitter.class.getName());

    private ConcurrentMap<String, Schema> _destTableSchemas;
    private Map<String, Boolean> _destTableCreated;

    private static final String CONFIG_THREADS = "threads";

    private final ExecutorService _executor;
    private final int _numOfCommitterThreads;

    private final BigQuery _bigquery;

    private static String sanitizeTableName(String tableName) {
        return tableName.replaceAll("[^A-Za-z0-9_]+", "_");
    }

    /**
     * Constructor for BigqueryBatchCommitter
     * @param properties configuration options
     */
    public BigqueryBatchCommitter(VerifiableProperties properties) {
        String credentialsPath = properties.getString("credentialsPath");
        String projectId = properties.getString("projectId");
        try {
            Credentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(credentialsPath));
            this._bigquery = BigQueryOptions.newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(credentials).build().getService();
        } catch (FileNotFoundException e) {
            LOG.error("Credentials path {} does not exist", credentialsPath);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOG.error("Unable to read credentials: {}", credentialsPath);
            throw new RuntimeException(e);
        }

        this._numOfCommitterThreads = properties.getInt(CONFIG_THREADS, 1);
        this._executor = Executors.newFixedThreadPool(_numOfCommitterThreads);

        this._destTableSchemas = new ConcurrentHashMap<>();
        this._destTableCreated = new HashMap<>();
    }

    private synchronized void createTableIfAbsent(String destination) {
        if (_destTableCreated.containsKey(destination)) {
            return;
        }
        String[] datasetTableNameRetention = destination.split("/");

        try {
            TableId tableId = TableId.of(datasetTableNameRetention[0], sanitizeTableName(datasetTableNameRetention[1]));
            TableDefinition tableDefinition;
            long partitionRetentionDays = Long.parseLong(datasetTableNameRetention[2]);
            if (partitionRetentionDays > 0) {
                tableDefinition = StandardTableDefinition.newBuilder()
                        .setSchema(_destTableSchemas.get(destination))
                        .setTimePartitioning(
                                TimePartitioning.of(TimePartitioning.Type.DAY, partitionRetentionDays * 86400000L))
                        .build();
            } else {
                tableDefinition = StandardTableDefinition.newBuilder()
                        .setSchema(_destTableSchemas.get(destination))
                        .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                        .build();
            }
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            if (_bigquery.getTable(tableId) != null) {
                LOG.debug("Table {} already exist", destination);
                return;
            }
            _bigquery.create(tableInfo);
            LOG.info("Table {} created successfully", destination);
        } catch (BigQueryException e) {
            LOG.warn("Failed to create table {} - {}", destination, e);
            throw e;
        }
    }

    /**
     * Allows to submit table schema for a lazy auto table creation
     * @param dest dataset and table
     * @param schema table schema
     */
    public void setDestTableSchema(String dest, Schema schema) {
        _destTableSchemas.putIfAbsent(dest, schema);
    }

    @Override
    public void commit(List<InsertAllRequest.RowToInsert> batch,
                       String destination,
                       List<SendCallback> ackCallbacks,
                       List<DatastreamRecordMetadata> recordMetadata,
                       List<Long> sourceTimestamps,
                       CommitCallback callback) {
        if (batch.isEmpty()) {
            return;
        }

        final Runnable committerTask = () -> {
            Exception exception = null;
            InsertAllResponse response = null;

            try {
                createTableIfAbsent(destination);

                TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
                SimpleDateFormat timeFmt = new SimpleDateFormat("yyyyMMdd");
                timeFmt.setTimeZone(utcTimeZone);

                String[] datasetTable = destination.split("/");
                TableId tableId = TableId.of(datasetTable[0],
                        sanitizeTableName(datasetTable[1]) + "$" + timeFmt.format(
                                Calendar.getInstance(utcTimeZone).getTimeInMillis()));

                LOG.debug("Committing a batch to dataset {} and table {}", datasetTable[0], sanitizeTableName(datasetTable[1]));
                long start = System.currentTimeMillis();

                response = _bigquery.insertAll(
                        InsertAllRequest.newBuilder(tableId, batch)
                                .build());

                DynamicMetricsManager.getInstance().createOrUpdateHistogram(
                        this.getClass().getSimpleName(),
                        recordMetadata.get(0).getTopic(),
                        "insertAllExecTime",
                         System.currentTimeMillis() - start);
            } catch (Exception e) {
                LOG.warn("Failed to insert a rows {}", response);
                exception = new DatastreamTransientException(e);
            }

            for (int i = 0; i < ackCallbacks.size(); i++) {
                if (exception != null) {
                    // entire batch failed
                    DynamicMetricsManager.getInstance().createOrUpdateMeter(
                            this.getClass().getSimpleName(),
                            recordMetadata.get(i).getTopic(),
                            "errorCount",
                            1);
                    ackCallbacks.get(i).onCompletion(recordMetadata.get(i), exception);
                    // force to check if table exists next time
                    _destTableCreated.remove(destination);
                } else {
                    Long key = Long.valueOf(i);
                    if (response != null && response.hasErrors() && response.getInsertErrors().containsKey(key)) {
                        LOG.warn("Failed to insert a row {} {}", i, response.getInsertErrors().get(key));
                        DynamicMetricsManager.getInstance().createOrUpdateMeter(
                                this.getClass().getSimpleName(),
                                recordMetadata.get(i).getTopic(),
                                "errorCount",
                                1);
                        ackCallbacks.get(i).onCompletion(recordMetadata.get(i),
                                new DatastreamTransientException(response.getInsertErrors().get(key).toString()));
                    } else {
                        DynamicMetricsManager.getInstance().createOrUpdateMeter(
                                this.getClass().getSimpleName(),
                                recordMetadata.get(i).getTopic(),
                                "commitCount",
                                1);
                        DynamicMetricsManager.getInstance().createOrUpdateHistogram(
                                this.getClass().getSimpleName(),
                                recordMetadata.get(i).getTopic(),
                                "eteLatency",
                                System.currentTimeMillis() - sourceTimestamps.get(i));
                        ackCallbacks.get(i).onCompletion(recordMetadata.get(i), null);
                    }
                }
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
                LOG.warn("Batch Committer shutdown timed out.");
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while awaiting committer termination.");
            Thread.currentThread().interrupt();
        }
        LOG.info("BQ Batch committer stopped.");
    }

}
