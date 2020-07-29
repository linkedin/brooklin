/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatch;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatchBuilder;

/**
 * This class builds batches of records to be committed to BQ eventually.
 */
public class BatchBuilder extends AbstractBatchBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BatchBuilder.class.getName());
    private static final String CONFIG_SCHEMA_REGISTRY = "schemaRegistry";

    private final int _maxBatchSize;
    private final int _maxBatchAge;
    private final int _maxInflightCommits;
    private final BigqueryBatchCommitter _committer;
    private final SchemaRegistry _schemaRegistry;

    /**
     * Constructor for BatchBuilder
     * @param maxBatchSize any batch bigger than this threshold will be committed to BQ.
     * @param maxBatchAge any batch older than this threshold will be committed to BQ.
     * @param maxInflightCommits maximum allowed batches in the commit backlog.
     * @param committer committer object.
     * @param queueSize queue size of the batch builder.
     * @param translatorProperties configuration options for translator.
     */
    public BatchBuilder(int maxBatchSize,
                        int maxBatchAge,
                        int maxInflightCommits,
                        BigqueryBatchCommitter committer,
                        int queueSize,
                        VerifiableProperties translatorProperties) {
        super(queueSize);
        this._maxBatchSize = maxBatchSize;
        this._maxBatchAge = maxBatchAge;
        this._maxInflightCommits = maxInflightCommits;
        this._committer = committer;
        this._schemaRegistry = new SchemaRegistry(
                new VerifiableProperties(translatorProperties.getDomainProperties(CONFIG_SCHEMA_REGISTRY)));
    }

    @Override
    public void run() {
        while (!isInterrupted()) {

            Package aPackage;
            try {
                aPackage = getNextPackage();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            try {
                if (aPackage.isDataPackage()) {
                    _registry.computeIfAbsent(aPackage.getTopic() + "-" + aPackage.getPartition(),
                            key -> new Batch(_maxBatchSize,
                                    _maxBatchAge,
                                    _maxInflightCommits,
                                    _schemaRegistry,
                                    _committer)).write(aPackage);
                } else {
                    // broadcast signal
                    for (Map.Entry<String, AbstractBatch> entry : _registry.entrySet()) {
                        entry.getValue().write(aPackage);
                    }
                }
                aPackage.markAsDelivered();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (aPackage.isDataPackage()) {
                    DynamicMetricsManager.getInstance().createOrUpdateMeter(
                            this.getClass().getSimpleName(),
                            aPackage.getTopic(),
                            "errorCount",
                            1);
                    LOG.error("Unable to write to batch {}", e);
                    aPackage.getAckCallback().onCompletion(new DatastreamRecordMetadata(
                            aPackage.getCheckpoint(), aPackage.getTopic(), aPackage.getPartition()), e);
                } else {
                    LOG.error("Unable to process flush signal {}", e);
                }
            }
        }
        LOG.info("Batch builder stopped.");
    }
}
