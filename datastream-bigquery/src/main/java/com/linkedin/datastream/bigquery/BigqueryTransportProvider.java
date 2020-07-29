/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatchBuilder;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider;

/**
 * This is a Bigquery Transport provider that writes events to specified bigquery table.
 */
public class BigqueryTransportProvider extends AbstractBufferedTransportProvider {

    private static final Logger LOG = LoggerFactory.getLogger(BigqueryTransportProvider.class.getName());

    private final BigqueryBatchCommitter _committer;

    private BigqueryTransportProvider(BigqueryTransportProviderBuilder builder) {
        super(builder._transportProviderName);

        this._committer = builder._committer;

        // initialize and start object builders
        for (int i = 0; i < builder._batchBuilderCount; i++) {
            _batchBuilders.add(new BatchBuilder(
                    builder._maxBatchSize,
                    builder._maxBatchAge,
                    builder._maxInflightBatchCommits,
                    builder._committer,
                    builder._batchBuilderQueueSize,
                    builder._translatorProperties));
        }
        for (AbstractBatchBuilder batchBuilder : _batchBuilders) {
            batchBuilder.start();
        }

        // send periodic flush signal to commit stale objects
        _scheduler.scheduleAtFixedRate(
                () -> {
                    for (AbstractBatchBuilder objectBuilder: _batchBuilders) {
                        LOG.info("Try flush signal sent.");
                        objectBuilder.assign(new com.linkedin.datastream.common.Package.PackageBuilder().buildTryFlushSignalPackage());
                    }
                },
                builder._maxBatchAge / 2,
                builder._maxBatchAge / 2,
                TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutdownCommitter() {
        _committer.shutdown();
    }

    /**
     * Builder class for {@link com.linkedin.datastream.bigquery.BigqueryTransportProvider}
     */
    public static class BigqueryTransportProviderBuilder {
        private String _transportProviderName;
        private int _batchBuilderQueueSize;
        private int _batchBuilderCount;
        private int _maxBatchSize;
        private int _maxBatchAge;
        private int _maxInflightBatchCommits;
        private BigqueryBatchCommitter _committer;
        private VerifiableProperties _translatorProperties;

        /**
         * Set the name of the transport provider
         */
        public BigqueryTransportProviderBuilder setTransportProviderName(String transportProviderName) {
            this._transportProviderName = transportProviderName;
            return this;
        }

        /**
         * Set batch builder's queue size
         */
        public BigqueryTransportProviderBuilder setBatchBuilderQueueSize(int batchBuilderQueueSize) {
            this._batchBuilderQueueSize = batchBuilderQueueSize;
            return this;
        }

        /**
         * Set number of batch builders
         */
        public BigqueryTransportProviderBuilder setBatchBuilderCount(int batchBuilderCount) {
            this._batchBuilderCount = batchBuilderCount;
            return this;
        }

        /**
         * Set max batch size
         */
        public BigqueryTransportProviderBuilder setMaxBatchSize(int maxBatchSize) {
            this._maxBatchSize = maxBatchSize;
            return this;
        }

        /**
         * Set max batch age
         */
        public BigqueryTransportProviderBuilder setMaxBatchAge(int maxBatchAge) {
            this._maxBatchAge = maxBatchAge;
            return this;
        }

        /**
         * Set max inflight commits
         */
        public BigqueryTransportProviderBuilder setMaxInflightBatchCommits(int maxInflightBatchCommits) {
            this._maxInflightBatchCommits = maxInflightBatchCommits;
            return this;
        }

        /**
         * Set batch committer
         */
        public BigqueryTransportProviderBuilder setCommitter(BigqueryBatchCommitter committer) {
            this._committer = committer;
            return this;
        }

        /**
         * Set translator configuration options
         */
        public BigqueryTransportProviderBuilder setTranslatorProperties(VerifiableProperties translatorProperties) {
            this._translatorProperties = translatorProperties;
            return this;
        }

        /**
         * Build the BigqueryTransportProvider.
         * @return
         *   BigqueryTransportProvider that is created.
         */
        public BigqueryTransportProvider build() {
            return new BigqueryTransportProvider(this);
        }
    }


}
