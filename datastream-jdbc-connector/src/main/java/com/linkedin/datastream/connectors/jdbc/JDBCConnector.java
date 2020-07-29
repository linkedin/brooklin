/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.jdbc;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wayfair.crypto.Passwords;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;

/**
 * implementation of {@link Connector}
 * JDBC connector to stream data from SQL tables
 */
public class JDBCConnector implements Connector {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCConnector.class);

    private static final String CONFIG_JDBC_USER = "user";
    private static final String CONFIG_JDBC_CREDENTIAL_NAME = "credentialName";
    private static final String CONFIG_CP_MIN_IDLE = "cpMinIdle";
    private static final String CONFIG_CP_MAX_IDLE = "cpMaxIdle";
    private static final String CONFIG_CHECKPOINT_STORE_URL = "checkpointStoreURL";
    private static final String CONFIG_CHECKPOINT_STORE_TOPIC = "checkpointStoreTopic";

    private static final String DS_CONFIG_MAX_TASKS = "maxTasks";
    private static final String DS_CONFIG_TABLE = "table";
    private static final String DS_CONFIG_INCREMENTING_COLUMN_NAME = "incrementingColumName";
    private static final String DS_CONFIG_INCREMENTING_INITIAL = "incrementingInitial";
    private static final String DS_CONFIG_QUERY = "query";
    private static final String DS_CONFIG_POLL_FREQUENCY_MS = "pollFrequencyMS";
    private static final String DS_CONFIG_MAX_POLL_ROWS = "maxPollRows";
    private static final String DS_CONFIG_MAX_FETCH_SIZE = "maxFetchSize";
    private static final String DS_CONFIG_DESTINATION_TOPIC = "destinationTopic";

    private static final int DEFAULT_MAX_POLL_RECORDS = 10000;
    private static final int DEFAULT_MAX_FETCH_SIZE = 1000;

    private final ConcurrentMap<String, DataSource> _datasources;
    private final Map<DatastreamTask, JDBCConnectorTask> _jdbcConnectorTasks;

    private final String _jdbcUser;
    private final String _jdbcUserPassword;
    private final int _cpMinIdle;
    private final int _cpMaxIdle;
    private final String _checkpointStoreUrl;
    private final String _checkpointStoreTopic;

    /**
     * constructor for JDBCConnector
     * @param config configuration options
     */
    public JDBCConnector(VerifiableProperties config) {
        _datasources = new ConcurrentHashMap<>();
        _jdbcConnectorTasks = new HashMap<>();

        if (!config.containsKey(CONFIG_JDBC_USER) || !config.containsKey(CONFIG_JDBC_CREDENTIAL_NAME)) {
            throw new RuntimeException("Config options user or password is missing");
        }

        if (!config.containsKey(CONFIG_CHECKPOINT_STORE_URL) || !config.containsKey(CONFIG_CHECKPOINT_STORE_TOPIC)) {
            throw new RuntimeException("Config options checkpointStoreURL or checkpointStoreTopic is missing");
        }

        _checkpointStoreUrl = config.getProperty(CONFIG_CHECKPOINT_STORE_URL);
        _checkpointStoreTopic = config.getProperty(CONFIG_CHECKPOINT_STORE_TOPIC);

        _jdbcUser = config.getString(CONFIG_JDBC_USER);

        try {
            _jdbcUserPassword = Passwords.get(config.getString(CONFIG_JDBC_CREDENTIAL_NAME));
        } catch (IOException e) {
            LOG.error("Unable to decrypt password.");
            throw new RuntimeException(e);
        }

        _cpMinIdle = config.getInt(CONFIG_CP_MIN_IDLE, 1);
        _cpMaxIdle = config.getInt(CONFIG_CP_MAX_IDLE, 5);
    }

    @Override
    public void start(CheckpointProvider checkpointProvider) {
    }

    @Override
    public synchronized void stop() {
        LOG.info("Stopping connector tasks...");
        _jdbcConnectorTasks.forEach((k, v) -> v.stop());
    }

    @Override
    public String getDestinationName(Datastream stream) {
        return stream.getMetadata().get(DS_CONFIG_DESTINATION_TOPIC);
    }

    @Override
    public synchronized void onAssignmentChange(List<DatastreamTask> tasks) {
        LOG.info("onAssignmentChange called with datastream tasks {}", tasks);
        Set<DatastreamTask> unassigned = new HashSet<>(_jdbcConnectorTasks.keySet());
        unassigned.removeAll(tasks);

        // Stop any unassigned tasks
        unassigned.forEach(t -> {
            _jdbcConnectorTasks.get(t).stop();
            _jdbcConnectorTasks.remove(t);
        });

        for (DatastreamTask task : tasks) {
            if (!_jdbcConnectorTasks.containsKey(task)) {
                LOG.info("Creating JDBC connector task for " + task);

                String connString = task.getDatastreamSource().getConnectionString();

                DataSource dataSource = _datasources.computeIfAbsent(
                        connString,
                        k -> {
                            BasicDataSource ds = new BasicDataSource();
                            ds.setUrl(connString);
                            ds.setUsername(_jdbcUser);
                            ds.setPassword(_jdbcUserPassword);
                            ds.setMinIdle(_cpMinIdle);
                            ds.setMaxIdle(_cpMaxIdle);
                            return ds;
                        }
                );

                StringMap metadata = task.getDatastreams().get(0).getMetadata();
                JDBCConnectorTask.JDBCConnectorTaskBuilder builder = new JDBCConnectorTask.JDBCConnectorTaskBuilder();
                builder.setDatastreamName(task.getDatastreams().get(0).getName())
                        .setEventProducer(task.getEventProducer())
                        .setDataSource(dataSource)
                        .setPollFrequencyMS(Long.parseLong(metadata.get(DS_CONFIG_POLL_FREQUENCY_MS)))
                        .setDestinationTopic(metadata.get(DS_CONFIG_DESTINATION_TOPIC))
                        .setCheckpointStoreURL(_checkpointStoreUrl)
                        .setCheckpointStoreTopic(_checkpointStoreTopic);

                if (metadata.containsKey(DS_CONFIG_QUERY)) {
                    builder.setQuery(metadata.get(DS_CONFIG_QUERY));
                }

                if (metadata.containsKey(DS_CONFIG_INCREMENTING_COLUMN_NAME)) {
                    builder.setIncrementingColumnName(metadata.get(DS_CONFIG_INCREMENTING_COLUMN_NAME));
                }

                if (metadata.containsKey(DS_CONFIG_INCREMENTING_INITIAL)) {
                    builder.setIncrementingInitial(Long.parseLong(metadata.get(DS_CONFIG_INCREMENTING_INITIAL)));
                }

                if (metadata.containsKey(DS_CONFIG_TABLE)) {
                    builder.setTable(metadata.get(DS_CONFIG_TABLE));
                }

                if (metadata.containsKey(DS_CONFIG_MAX_POLL_ROWS)) {
                    try {
                      builder.setMaxPollRows(Integer.parseInt(metadata.get(DS_CONFIG_MAX_POLL_ROWS)));
                    } catch (NumberFormatException e) {
                        LOG.warn(DS_CONFIG_MAX_POLL_ROWS + " config value is not a valid number. Using the default value " + DEFAULT_MAX_POLL_RECORDS);
                        builder.setMaxPollRows(DEFAULT_MAX_POLL_RECORDS);
                    }
                } else {
                    builder.setMaxPollRows(DEFAULT_MAX_POLL_RECORDS);
                }

                if (metadata.containsKey(DS_CONFIG_MAX_FETCH_SIZE)) {
                    try {
                        builder.setMaxFetchSize(Integer.parseInt(metadata.get(DS_CONFIG_MAX_FETCH_SIZE)));
                    } catch (NumberFormatException e) {
                        LOG.warn(DS_CONFIG_MAX_FETCH_SIZE + " config value is not a valid number. Using the default value " + DEFAULT_MAX_FETCH_SIZE);
                        builder.setMaxFetchSize(DEFAULT_MAX_FETCH_SIZE);
                    }
                } else {
                    builder.setMaxFetchSize(DEFAULT_MAX_FETCH_SIZE);
                }

                JDBCConnectorTask jdbcConnectorTask = builder.build();
                _jdbcConnectorTasks.put(task, jdbcConnectorTask);
                jdbcConnectorTask.start();
            }
        }
    }

    @Override
    public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams) throws DatastreamValidationException {
        StringMap metadata = stream.getMetadata();
        if (metadata.containsKey(DS_CONFIG_MAX_TASKS)) {
            if (Integer.parseInt(stream.getMetadata().get(DS_CONFIG_MAX_TASKS)) != 1) {
                throw new DatastreamValidationException("maxTasks other than value 1 is not supported.");
            }

        } else {
            metadata.put(DS_CONFIG_MAX_TASKS, "1");
        }

        for (String param : Arrays.asList(DS_CONFIG_POLL_FREQUENCY_MS,
                DS_CONFIG_DESTINATION_TOPIC,
                DS_CONFIG_INCREMENTING_COLUMN_NAME,
                DS_CONFIG_INCREMENTING_INITIAL)) {
            if (!metadata.containsKey(param)) {
                throw new DatastreamValidationException(param + " is missing in the config");
            }
        }

        try {
            Integer.parseInt(metadata.get(DS_CONFIG_INCREMENTING_INITIAL));
        } catch (NumberFormatException e) {
            throw new DatastreamValidationException(DS_CONFIG_INCREMENTING_INITIAL + " config value is not an integer");
        }

        if ((!metadata.containsKey(DS_CONFIG_QUERY) || metadata.get(DS_CONFIG_QUERY).length() == 0) &&
                (!metadata.containsKey(DS_CONFIG_TABLE) || metadata.get(DS_CONFIG_TABLE).length() == 0)) {
            throw new DatastreamValidationException("One of the two config options " + DS_CONFIG_QUERY + " and " + DS_CONFIG_TABLE + " must be provided.");
        }

        metadata.put(DatastreamMetadataConstants.IS_CONNECTOR_MANAGED_DESTINATION_KEY, Boolean.TRUE.toString());

        stream.setMetadata(metadata);
    }

}