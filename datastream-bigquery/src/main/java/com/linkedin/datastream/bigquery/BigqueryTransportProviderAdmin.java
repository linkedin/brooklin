/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.time.Duration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;

/**
 * {@link TransportProviderAdmin} implementation for {@link BigqueryTransportProvider}
 *
 * <ul>
 *  <li>Initializes {@link BigqueryTransportProvider}</li>
 *  <li>Sets up the correct destination connection string/bq table</li>
 * </ul>
 */
public class BigqueryTransportProviderAdmin implements TransportProviderAdmin {
    private static final Logger LOG = LoggerFactory.getLogger(BigqueryTransportProviderAdmin.class);

    private static final String CONFIG_BATCHBUILDER_QUEUE_SIZE = "batchBuilderQueueSize";
    private static final String CONFIG_BATCHBUILDER_THREAD_COUNT = "batchBuilderThreadCount";
    private static final String CONFIG_MAX_BATCH_SIZE = "maxBatchSize";
    private static final String CONFIG_MAX_BATCH_AGE = "maxBatchAge";
    private static final String CONFIG_MAX_INFLIGHT_COMMITS = "maxInflightCommits";

    private static final String CONFIG_TRANSLATOR_DOMAIN_PREFIX = "translator";

    private static final String CONFIG_COMMITTER_DOMAIN_PREFIX = "committer";

    private BigqueryTransportProvider _transportProvider;

    /**
     * Constructor for BigqueryTransportProviderAdmin.
     * @param props TransportProviderAdmin configuration properties, e.g. number of committer threads, file format.
     */
    public BigqueryTransportProviderAdmin(String transportProviderName, Properties props) {
        VerifiableProperties tpProperties = new VerifiableProperties(props);

        VerifiableProperties committerProperties = new VerifiableProperties(tpProperties.getDomainProperties(
                CONFIG_COMMITTER_DOMAIN_PREFIX, false));

        _transportProvider = new BigqueryTransportProvider.BigqueryTransportProviderBuilder()
                .setTransportProviderName(transportProviderName)
                .setBatchBuilderQueueSize(tpProperties.getInt(CONFIG_BATCHBUILDER_QUEUE_SIZE, 1000))
                .setBatchBuilderCount(tpProperties.getInt(CONFIG_BATCHBUILDER_THREAD_COUNT, 5))
                .setMaxBatchSize(tpProperties.getInt(CONFIG_MAX_BATCH_SIZE, 100000))
                .setMaxBatchAge(tpProperties.getInt(CONFIG_MAX_BATCH_AGE, 500))
                .setMaxInflightBatchCommits(tpProperties.getInt(CONFIG_MAX_INFLIGHT_COMMITS, 1))
                .setCommitter(new BigqueryBatchCommitter(committerProperties))
                .setTranslatorProperties(new VerifiableProperties(tpProperties.getDomainProperties(CONFIG_TRANSLATOR_DOMAIN_PREFIX)))
                .build();
    }

    @Override
    public TransportProvider assignTransportProvider(DatastreamTask task) {
        return _transportProvider;
    }

    @Override
    public void unassignTransportProvider(DatastreamTask task) {
    }

    @Override
    public void initializeDestinationForDatastream(Datastream datastream, String destinationName)
            throws DatastreamValidationException {
        if (!datastream.hasDestination()) {
            datastream.setDestination(new DatastreamDestination());
        }

        if (!datastream.getMetadata().containsKey("dataset")) {
            throw new DatastreamValidationException("Metadata dataset is not set in the datastream definition.");
        }

        String destination = datastream.getMetadata().get("dataset")
                + "/"
                + (datastream.getMetadata().containsKey("tableSuffix") ? datastream.getMetadata().get("tableSuffix") : "");

        datastream.getDestination().setConnectionString(destination);
    }

    @Override
    public void createDestination(Datastream datastream) {
    }

    @Override
    public void dropDestination(Datastream datastream) {
    }

    @Override
    public Duration getRetention(Datastream datastream) {
        return Duration.ofSeconds(0);
    }
}
