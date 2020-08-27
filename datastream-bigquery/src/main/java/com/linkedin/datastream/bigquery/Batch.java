/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.Schema;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import com.linkedin.datastream.bigquery.translator.RecordTranslator;
import com.linkedin.datastream.bigquery.translator.SchemaTranslator;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatch;

/**
 * This class populates a batch of BQ rows to be committed.
 */
public class Batch extends AbstractBatch {

    private final int _maxBatchSize;
    private final int _maxBatchAge;
    private final BigqueryBatchCommitter _committer;

    private final List<InsertAllRequest.RowToInsert> _batch;
    private long _batchCreateTimeStamp;

    private org.apache.avro.Schema _avroSchema;
    private Schema _schema;
    private SchemaRegistry _schemaRegistry;
    private String _destination;
    private KafkaAvroDeserializer _deserializer;

    /**
     * Constructor for Batch.
     * @param maxBatchSize any batch bigger than this threshold will be committed to BQ.
     * @param maxBatchAge any batch older than this threshold will be committed to BQ.
     * @param maxInflightWriteLogCommits maximum allowed batches in the commit backlog
     * @param schemaRegistry schema registry client object
     * @param committer committer object
     */
    public Batch(int maxBatchSize,
                 int maxBatchAge,
                 int maxInflightWriteLogCommits,
                 SchemaRegistry schemaRegistry,
                 BigqueryBatchCommitter committer) {
        super(maxInflightWriteLogCommits);
        this._maxBatchSize = maxBatchSize;
        this._maxBatchAge = maxBatchAge;
        this._committer = committer;
        this._batch = new ArrayList<>();
        this._batchCreateTimeStamp = System.currentTimeMillis();
        this._schema = null;
        this._destination = null;
        this._schemaRegistry = schemaRegistry;
        this._deserializer = _schemaRegistry.getDeserializer();
    }

    private void reset() {
        _batch.clear();
        _destination = null;
        _ackCallbacks.clear();
        _recordMetadata.clear();
        _sourceTimestamps.clear();
        _batchCreateTimeStamp = System.currentTimeMillis();
    }

    @Override
    public void write(Package aPackage) throws InterruptedException {
        if (aPackage.isDataPackage()) {

            // skip null records
            if (aPackage.getRecord().getValue() == null) {
                LOG.info("Null record received from topic {}, partition {}, and offset {}",
                        aPackage.getTopic(), aPackage.getPartition(), aPackage.getOffset());
                DynamicMetricsManager.getInstance().createOrUpdateMeter(
                        this.getClass().getSimpleName(),
                        aPackage.getTopic(),
                        "nullRecordsCount",
                        1);
                aPackage.getAckCallback().onCompletion(new DatastreamRecordMetadata(
                        aPackage.getCheckpoint(), aPackage.getTopic(), aPackage.getPartition()), null);
                return;
            }

            if (_destination == null) {
                String[] datasetTableSuffix = aPackage.getDestination().split("/");
                if (datasetTableSuffix.length == 2) {
                    _destination = datasetTableSuffix[0] + "/" + aPackage.getTopic() + datasetTableSuffix[1];
                } else {
                    _destination = datasetTableSuffix[0] + "/" + aPackage.getTopic();
                }
            }

            if (_schema == null) {
                _avroSchema = _schemaRegistry.getSchemaByTopic(aPackage.getTopic());
                _schema = SchemaTranslator.translate(_avroSchema);
                _committer.setDestTableSchema(_destination, _schema);
            }

            GenericRecord record;
            try {
                record = (GenericRecord) _deserializer.deserialize(
                        aPackage.getTopic(),
                        (byte[]) aPackage.getRecord().getValue());
            } catch (SerializationException e) {
                if (e.getCause() instanceof BufferUnderflowException) {
                    LOG.warn("Skipping message at Topic {} - Partition {} - Offset {} - Reason {} - Exception {}",
                            aPackage.getTopic(),
                            aPackage.getPartition(),
                            aPackage.getOffset(),
                            e.getMessage(),
                            e.getCause().getClass());
                    DynamicMetricsManager.getInstance().createOrUpdateMeter(
                            this.getClass().getSimpleName(),
                            aPackage.getTopic(),
                            "deserializerErrorCount",
                            1);
                    aPackage.getAckCallback().onCompletion(new DatastreamRecordMetadata(
                            aPackage.getCheckpoint(), aPackage.getTopic(), aPackage.getPartition()), null);
                    return;
                } else {
                    throw e;
                }
            }

            _batch.add(RecordTranslator.translate(record, _avroSchema));

            _ackCallbacks.add(aPackage.getAckCallback());
            _recordMetadata.add(new DatastreamRecordMetadata(aPackage.getCheckpoint(),
                    aPackage.getTopic(),
                    aPackage.getPartition()));
            _sourceTimestamps.add(aPackage.getTimestamp());
        } else if (aPackage.isTryFlushSignal() || aPackage.isForceFlushSignal()) {
            if (_batch.isEmpty()) {
                LOG.debug("Nothing to flush.");
                return;
            }
        }

        if (_batch.size() >= _maxBatchSize ||
                System.currentTimeMillis() - _batchCreateTimeStamp >= _maxBatchAge ||
                aPackage.isForceFlushSignal()) {
            waitForRoomInCommitBacklog();
            incrementInflightWriteLogCommits();
            _committer.commit(
                    new ArrayList<>(_batch),
                    _destination,
                    new ArrayList<>(_ackCallbacks),
                    new ArrayList<>(_recordMetadata),
                    new ArrayList<>(_sourceTimestamps),
                    () -> decrementInflightWriteLogCommitsAndNotify()
            );
            reset();

            if (aPackage.isForceFlushSignal()) {
                waitForCommitBacklogToClear();
            }
        }
    }
}
