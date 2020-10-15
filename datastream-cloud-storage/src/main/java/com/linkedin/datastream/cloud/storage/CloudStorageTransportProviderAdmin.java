/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage;

import java.time.Duration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.cloud.storage.committer.ObjectCommitter;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.DatastreamTask;

import com.linkedin.datastream.server.api.connector.DatastreamValidationException;

import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;

/**
 * {@link TransportProviderAdmin} implementation for {@link CloudStorageTransportProvider}
 *
 * <ul>
 *  <li>Initializes {@link CloudStorageTransportProvider}</li>
 *  <li>Sets up the correct destination connection string/storage bucket</li>
 * </ul>
 */
public class CloudStorageTransportProviderAdmin implements TransportProviderAdmin {

    public static final Logger LOG = LoggerFactory.getLogger(CloudStorageTransportProviderAdmin.class);

    public static final String CONFIG_OBJECTBUILDER_QUEUE_SIZE = "objectBuilderQueueSize";
    public static final String CONFIG_OBJECTBUILDER_THREAD_COUNT = "objectBuilderThreadCount";
    public static final String CONFIG_MAX_FILE_SIZE = "maxFileSize";
    public static final String CONFIG_MAX_FILE_AGE = "maxFileAge";
    public static final String CONFIG_INFLIGHT_WRITE_LOG_COMMITS = "inflightCommits";

    public static final String CONFIG_IO_DOMAIN_PREFIX = "io";
    public static final String CONFIG_IO_CLASS = "class";
    public static final String CONFIG_IO_DIRECTORY = "directory";

    public static final String CONFIG_COMMITTER_DOMAIN_PREFIX = "committer";
    public static final String CONFIG_COMMITTER_CLASS = "class";

    private CloudStorageTransportProvider _transportProvider;

    /**
     * Constructor for CloudStorageTransportProviderAdmin.
     * @param props TransportProviderAdmin configuration properties, e.g. number of committer threads, file format.
     */
    public CloudStorageTransportProviderAdmin(String transportProviderName, Properties props) {
        VerifiableProperties tpProperties = new VerifiableProperties(props);

        VerifiableProperties ioProperties = new VerifiableProperties(tpProperties.getDomainProperties(
                CONFIG_IO_DOMAIN_PREFIX, false));

        VerifiableProperties committerProperties = new VerifiableProperties(tpProperties.getDomainProperties(
                CONFIG_COMMITTER_DOMAIN_PREFIX, false));

        ObjectCommitter committer = ReflectionUtils.createInstance(committerProperties.getString(
                CONFIG_COMMITTER_CLASS), committerProperties);

        _transportProvider = new CloudStorageTransportProvider.CloudStorageTransportProviderBuilder()
                .setTransportProviderName(transportProviderName)
                .setObjectBuilderQueueSize(tpProperties.getInt(CONFIG_OBJECTBUILDER_QUEUE_SIZE, 1000))
                .setObjectBuilderCount(tpProperties.getInt(CONFIG_OBJECTBUILDER_THREAD_COUNT, 5))
                .setIOClass(ioProperties.getString(CONFIG_IO_CLASS))
                .setIOProperties(ioProperties)
                .setLocalDirectory(ioProperties.getString(CONFIG_IO_DIRECTORY))
                .setMaxFileSize(tpProperties.getLong(CONFIG_MAX_FILE_SIZE, 100000))
                .setMaxFileAge(tpProperties.getInt(CONFIG_MAX_FILE_AGE, 500))
                .setMaxInflightWriteLogCommits(tpProperties.getInt(CONFIG_INFLIGHT_WRITE_LOG_COMMITS, 1))
                .setObjectCommitter(committer)
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

        if (!datastream.getMetadata().containsKey("bucket")) {
            throw new DatastreamValidationException("Metadata bucket is not set in the datastream definition.");
        }


        String destination;
        if (datastream.getMetadata().containsKey("application")) {
            destination = datastream.getMetadata().get("bucket")
                    + "/"
                    + datastream.getMetadata().get("application");
        } else {
            destination = datastream.getMetadata().get("bucket");
        }

        if (!datastream.getMetadata().containsKey("excludeDatastreamName") ||
                datastream.getMetadata().get("excludeDatastreamName").equals("false")) {
            destination += "/" + datastream.getName();
        }

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
