/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.connectors.jdbc.translator.JdbcCommon;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;
import com.linkedin.datastream.server.providers.CustomCheckpointProvider;
import com.linkedin.datastream.server.providers.KafkaCustomCheckpointProvider;

/**
 * Connector task that reads data from SQL server and streams to the specified transportprovider.
 */
public class JDBCConnectorTask {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCConnectorTask.class);

    private final ScheduledExecutorService _scheduler = Executors.newScheduledThreadPool(1);
    private final String _datastreamName;
    private final DataSource _dataSource;
    private final String _query;
    private final String _incrementingColumnName;
    private final long _incrementingInitial;
    private final String _table;
    private final long _pollFrequencyMS;
    private final String _checkpointStorerURL;
    private final String _checkpointStoreTopic;
    private final String _destinationTopic;
    private final FlushlessEventProducerHandler<Long> _flushlessProducer;
    private final int _maxPollRows;
    private final int _maxFetchSize;

    private String _id;
    private CustomCheckpointProvider<Long> _checkpointProvider;
    private AtomicBoolean _resetToSafeCommit;

    private JDBCConnectorTask(JDBCConnectorTaskBuilder builder) {
        this._datastreamName = builder._datastreamName;
        this._dataSource = builder._dataSource;
        this._query = builder._query;
        this._incrementingColumnName = builder._incrementingColumnName;
        this._incrementingInitial = builder._incrementingInitial;
        this._table = builder._table;
        this._pollFrequencyMS = builder._pollFrequencyMS;
        this._flushlessProducer = new FlushlessEventProducerHandler<>(builder._producer);
        this._checkpointStorerURL = builder._checkpointStorerURL;
        this._checkpointStoreTopic = builder._checkpointStoreTopic;
        this._destinationTopic = builder._destinationTopic;
        this._maxPollRows = builder._maxPollRows;
        this._maxFetchSize = builder._maxFetchSize;

        this._checkpointProvider = null;
        this._resetToSafeCommit = new AtomicBoolean(false);
    }

    private String generateID() {
        String idString = _datastreamName + " " + _incrementingColumnName + " " + _destinationTopic;
        long hash = idString.hashCode();
        return String.valueOf(hash > 0 ? hash : -hash);
    }

    private void processResults(ResultSet resultSet) throws SQLException, IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(5 * 1024 * 1024);
        JdbcCommon.convertToAvroStream(resultSet, outputStream, true);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(JdbcCommon.createSchema(resultSet));
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataFileStream<GenericRecord> avroStream = new DataFileStream<>(inputStream, reader);

        while (avroStream.hasNext()) {
            GenericRecord record = avroStream.next();
            Long checkpoint = (record.get(_incrementingColumnName) instanceof Integer) ?
                    Long.valueOf((Integer) record.get(_incrementingColumnName)) :
                    (Long) record.get(_incrementingColumnName);

            HashMap<String, String> meta = new HashMap<>();
            meta.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
            BrooklinEnvelope envelope = new BrooklinEnvelope(checkpoint, record, null, meta);
            DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
            builder.addEvent(envelope);
            builder.setEventsSourceTimestamp(System.currentTimeMillis());

            _flushlessProducer.send(builder.build(), _id, 0, checkpoint,
                    (DatastreamRecordMetadata metadata, Exception exception) -> {
                        if (exception != null) {
                            _resetToSafeCommit.set(true);
                            LOG.warn("failed to send row {}. {}", checkpoint, exception);
                        }
                    });
            _checkpointProvider.updateCheckpoint(checkpoint);
        }

    }

    private synchronized void mayCommitCheckpoint() {
        Optional<Long> safeCheckpoint = _flushlessProducer.getAckCheckpoint(_id, 0);
        if (safeCheckpoint.isPresent()) {
            if (_resetToSafeCommit.get()) {
                _checkpointProvider.rewindTo(safeCheckpoint.get());
                _resetToSafeCommit.set(false);
            } else {
                _checkpointProvider.commit(safeCheckpoint.get());
            }
        }
    }

    private Long getInitialCheckpoint() {
        return _incrementingInitial;
    }

    private String generateStatement() {
        String suffix = " WHERE " + _incrementingColumnName + " > ? ORDER BY " + _incrementingColumnName + " ASC";
        return (_query != null) ?
                _query + suffix :
                "SELECT * FROM " + _table + " WITH (NOLOCK)" + suffix;
    }

    private void poll() {
        LOG.info("poll initiated for {}", _datastreamName);

        mayCommitCheckpoint();
        Long checkpoint = _checkpointProvider.getSafeCheckpoint();
        checkpoint = (checkpoint == null) ? getInitialCheckpoint() : checkpoint;

        LOG.info("start checkpoint is {}", checkpoint);

        try (Connection conn = _dataSource.getConnection()) {
            try (PreparedStatement preparedStatement = conn.prepareStatement(generateStatement())) {
                preparedStatement.setMaxRows(_maxPollRows);
                preparedStatement.setFetchSize(_maxFetchSize);
                preparedStatement.setLong(1, checkpoint);

                try (ResultSet rs = preparedStatement.executeQuery()) {
                    this.processResults(rs);
                }
            }
        } catch (SQLException | IOException e) {
            LOG.warn("Failed to poll {}", e);
        }
    }

    /**
     * start the task
     */
    public void start() {
        _id = generateID();
        this._checkpointProvider = new KafkaCustomCheckpointProvider(_id, _checkpointStorerURL, _checkpointStoreTopic);
        _scheduler.scheduleWithFixedDelay(() -> {
                    try {
                        this.poll();
                    } catch (Exception e) {
                        LOG.warn("Failed poll. {}", e);
                    }
                },
                _pollFrequencyMS,
                _pollFrequencyMS,
                TimeUnit.MILLISECONDS);
    }

    /**
     * stop the task
     */
    public void stop() {
        LOG.info("Stopping datastream {}...", _datastreamName);

        _scheduler.shutdownNow();
        try {
            if (!_scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Task scheduler shutdown timed out.");
            }
            mayCommitCheckpoint();
            _checkpointProvider.close();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while awaiting task scheduler termination.");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * builder class for JDBCConnectorTask
     */
    public static class JDBCConnectorTaskBuilder {
        private String _datastreamName;
        private DatastreamEventProducer _producer;

        private DataSource _dataSource;
        private String _query = null;
        private String _table = null;
        private String _incrementingColumnName = null;
        private long _incrementingInitial;
        private long _pollFrequencyMS = 60000;
        private int _maxPollRows;
        private int _maxFetchSize;

        private String _checkpointStorerURL;
        private String _checkpointStoreTopic;
        private String _destinationTopic;

        /**
         * set the datastream name
         * @param datastreamName datastream name
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setDatastreamName(String datastreamName) {
            this._datastreamName = datastreamName;
            return this;
        }

        /**
         * set the sql data source
         * @param dataSource datasource
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setDataSource(DataSource dataSource) {
            this._dataSource = dataSource;
            return this;
        }

        /**
         * set the query to use
         * @param query sql query
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setQuery(String query) {
            this._query = query;
            return this;
        }

        /**
         * set the incrementing column name for checkpoiniting
         * @param incrementingColumnName column name
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setIncrementingColumnName(String incrementingColumnName) {
            this._incrementingColumnName = incrementingColumnName;
            return this;
        }

        /**
         * set the table to query
         * @param table table name
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setTable(String table) {
            this._table = table;
            return this;
        }

        /**
         * set the poll frequency
         * @param pollFrequencyMS poll frequency in milli seconds
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setPollFrequencyMS(long pollFrequencyMS) {
            this._pollFrequencyMS = pollFrequencyMS;
            return this;
        }

        /**
         * set the event producer
         * @param producer event producer
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setEventProducer(DatastreamEventProducer producer) {
            this._producer = producer;
            return this;
        }

        /**
         * set the checkpoint store url
         * @param checkpointStoreURL checkpoint store url
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setCheckpointStoreURL(String checkpointStoreURL) {
            this._checkpointStorerURL = checkpointStoreURL;
            return this;
        }

        /**
         * set the checkpoint store topic
         * @param checkpointStoreTopic topic name
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setCheckpointStoreTopic(String checkpointStoreTopic) {
            this._checkpointStoreTopic = checkpointStoreTopic;
            return this;
        }

        /**
         * set initial checkpoint
         * @param incrementingInitial initial checkpoint value
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setIncrementingInitial(long incrementingInitial) {
            this._incrementingInitial = incrementingInitial;
            return this;
        }

        /**
         * set the destination topic
         * @param destinationTopic topic to produce to
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setDestinationTopic(String destinationTopic) {
            this._destinationTopic = destinationTopic;
            return this;
        }

        /**
         * set max rows to poll
         * @param maxPollRows max number of rows to poll
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setMaxPollRows(int maxPollRows) {
            this._maxPollRows = maxPollRows;
            return this;
        }

        /**
         * set max fetch size
         * @param maxFetchSize max rows to fetch in each network call
         * @return instance of JDBCConnectorTaskBuilder
         */
        public JDBCConnectorTaskBuilder setMaxFetchSize(int maxFetchSize) {
            this._maxFetchSize = maxFetchSize;
            return this;
        }

        /**
         * build connector task
         * @return instance of JDBCConnectorTask
         */
        public JDBCConnectorTask build() {
            return new JDBCConnectorTask(this);
        }
    }
}
