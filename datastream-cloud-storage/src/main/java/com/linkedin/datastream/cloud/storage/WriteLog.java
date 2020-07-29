/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import com.linkedin.datastream.cloud.storage.committer.ObjectCommitter;
import com.linkedin.datastream.cloud.storage.io.File;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.common.VerifiableProperties;

import com.linkedin.datastream.metrics.DynamicMetricsManager;

/**
 * This class writes the messages to local file. Which can be committed to cloud storage later.
 */
public class WriteLog {
    private static final Logger LOG = LoggerFactory.getLogger(WriteLog.class.getName());

    private final String _ioClass;
    private final VerifiableProperties _ioProperties;
    private final String _localDir;
    private final long _maxFileSize;
    private final int _maxFileAge;
    private final ObjectCommitter _committer;
    private final int _maxInflightWriteLogCommits;

    private final Meter _writeRateMeter;

    private File _file;
    private String _destination;
    private String _topic;
    private long _partition;
    private long _minOffset;
    private long _maxOffset;
    private List<SendCallback> _ackCallbacks;
    private List<DatastreamRecordMetadata> _recordMetadata;

    private int _inflightWriteLogCommits;
    private Object _counterLock;

    private long _fileStartTime;

    /**
     * Constructor for WriteLog.
     * @param ioClass one of the implementations of {@link com.linkedin.datastream.cloud.storage.io.File}
     * @param ioProperties configuration options for the io class
     * @param localDir local directory where the objects are created before committing to cloud storage
     * @param maxFileSize max size of the object
     * @param maxFileAge max age of the object
     * @param maxInflightWriteLogCommits max commit backlog
     * @param committer object committer
     */
    public WriteLog(String ioClass,
                    VerifiableProperties ioProperties,
                    String localDir,
                    long maxFileSize,
                    int maxFileAge,
                    int maxInflightWriteLogCommits,
                    ObjectCommitter committer) {
        this._ioClass = ioClass;
        this._ioProperties = ioProperties;
        this._maxInflightWriteLogCommits = maxInflightWriteLogCommits;
        this._inflightWriteLogCommits = 0;
        this._maxFileSize = maxFileSize;
        this._maxFileAge = maxFileAge;
        this._file = null;
        this._fileStartTime = System.currentTimeMillis();
        this._localDir = localDir;
        this._committer = committer;
        this._ackCallbacks = new ArrayList<>();
        this._recordMetadata = new ArrayList<>();
        this._destination = null;
        this._topic = null;
        this._partition = 0;
        this._minOffset = Long.MAX_VALUE;
        this._maxOffset = Long.MIN_VALUE;
        this._counterLock = new Object();
        this._writeRateMeter = DynamicMetricsManager.getInstance().registerMetric(this.getClass().getSimpleName(),
                "writeRate", Meter.class);
    }

    private String getFilePath(String topic, String partition) {
        return new StringBuilder()
                .append(_localDir)
                .append("/")
                .append(topic)
                .append("+")
                .append(partition)
                .append("+")
                .append(System.currentTimeMillis()).toString();
    }

    private void waitForRoomInCommitBacklog() throws InterruptedException {
        synchronized (_counterLock) {
            if (_inflightWriteLogCommits >= _maxInflightWriteLogCommits) {
                LOG.info("Waiting for room in commit backlog, current inflight commits {} ",
                        _inflightWriteLogCommits);
            }
            while (_inflightWriteLogCommits >= _maxInflightWriteLogCommits) {
                try {
                    _counterLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    private void waitForCommitBacklogToClear() throws InterruptedException {
        synchronized (_counterLock) {
            if (_inflightWriteLogCommits > 0) {
                LOG.info("Waiting for the commit backlog to clear.");
            }
            while (_inflightWriteLogCommits > 0) {
                try {
                    _counterLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    private void incrementInflightWriteLogCommits() {
        synchronized (_counterLock) {
            _inflightWriteLogCommits++;
        }
    }

    private void decrementInflightWriteLogCommitsAndNotify() {
        synchronized (_counterLock) {
            _inflightWriteLogCommits--;
            _counterLock.notify();
        }
    }

    private void reset() {
        _file = null;
        _destination = null;
        _topic = null;
        _partition = 0;
        _minOffset = Long.MAX_VALUE;
        _maxOffset = Long.MIN_VALUE;
        _ackCallbacks.clear();
        _recordMetadata.clear();
    }

    /**
     * writes the given record in the package to the write log
     * @param aPackage package to be written
     * @throws IOException
     */
    public void write(Package aPackage) throws IOException, InterruptedException {
        if (aPackage.isDataPackage()) {
            final String filePath = getFilePath(aPackage.getTopic(), String.valueOf(aPackage.getPartition()));
            if (_file == null) {
                _file = ReflectionUtils.createInstance(_ioClass, filePath, _ioProperties);
                _destination = aPackage.getDestination();
                _topic = aPackage.getTopic();
                _partition = aPackage.getPartition();
                _fileStartTime = System.currentTimeMillis();
            }
            _writeRateMeter.mark();
            _file.write(aPackage);
            _maxOffset = (aPackage.getOffset() > _maxOffset) ? aPackage.getOffset() : _maxOffset;
            _minOffset = (aPackage.getOffset() < _minOffset) ? aPackage.getOffset() : _minOffset;
            _ackCallbacks.add(aPackage.getAckCallback());
            _recordMetadata.add(new DatastreamRecordMetadata(aPackage.getCheckpoint(),
                    aPackage.getTopic(),
                    aPackage.getPartition()));
        } else if (aPackage.isTryFlushSignal() || aPackage.isForceFlushSignal()) {
            if (_file == null) {
                LOG.debug("Nothing to flush.");
                return;
            }
        }
        if (_file.length() >= _maxFileSize ||
                System.currentTimeMillis() - _fileStartTime >= _maxFileAge ||
                aPackage.isForceFlushSignal()) {
            waitForRoomInCommitBacklog();
            incrementInflightWriteLogCommits();
            _file.close();
            _committer.commit(
                    _file.getPath(),
                    _file.getFileFormat(),
                    _destination,
                    _topic,
                    _partition,
                    _minOffset,
                    _maxOffset,
                    new ArrayList<>(_ackCallbacks),
                    new ArrayList<>(_recordMetadata),
                    () -> decrementInflightWriteLogCommitsAndNotify()
                    );
            reset();

            if (aPackage.isForceFlushSignal()) {
                waitForCommitBacklogToClear();
            }
        }
    }
}
