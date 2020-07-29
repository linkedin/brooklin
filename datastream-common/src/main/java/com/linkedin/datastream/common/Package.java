/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Record and it's associated metadata
 */
public class Package extends PackageTracking {

    /**
     * defines the package type -> data or signal
     */
    public enum MessageType {
        DATA,
        TRY_FLUSH_SIGNAL,
        FORCE_FLUSH_SIGNAL
    };

    private MessageType _messageType;
    private Record _record;
    private String _destination;
    private SendCallback _ackCallback;
    private String _topic;
    private int _partition;
    private long _offset;
    private long _timestamp;
    private String _checkpoint;

    private Package() {
        super();
    }

    private Package(PackageBuilder builder) {
        this();
        this._messageType = MessageType.DATA;
        this._record = builder._record;
        this._topic = builder._topic;
        this._partition = Integer.parseInt(builder._partition);
        this._offset = Long.parseLong(builder._offset);
        this._timestamp = builder._timestamp;
        this._destination = builder._destination;
        this._ackCallback = builder._ackCallback;
        this._checkpoint = builder._checkpoint;
    }

    private void validateCall() {
        if (_messageType != MessageType.DATA) {
            throw new IllegalArgumentException("Not a valid call on non DATA package.");
        }
    }

    /**
     * get the record
     * @return record
     */
    public Record getRecord() {
        validateCall();
        return _record;
    }

    /**
     * get the source topic of the record
     * @return topic
     */
    public String getTopic() {
        validateCall();
        return _topic;
    }

    /**
     * get the source partition of the record
     * @return partition
     */
    public int getPartition() {
        validateCall();
        return _partition;
    }

    /**
     * get the source offset of the record
     * @return offset
     */
    public long getOffset() {
        validateCall();
        return _offset;
    }

    /**
     * get the timestamp
     * @return timestamp
     */
    public long getTimestamp() {
        validateCall();
        return _timestamp;
    }

    /**
     * get the record destination
     * @return destination
     */
    public String getDestination() {
        validateCall();
        return _destination;
    }

    /**
     * get the callback acknowledge the success or failure of the send
     * @return Ack callback
     */
    public SendCallback getAckCallback() {
        validateCall();
        return _ackCallback;
    }

    /**
     * get the checkpoint
     * @return checkpoint
     */
    public String getCheckpoint() {
        validateCall();
        return _checkpoint;
    }

    @Override
    public int hashCode() {
        return (getTopic() + getPartition()).hashCode();
    }

    /**
     * returns true if the package is data package
     * @return true if it is data package
     */
    public boolean isDataPackage() {
        return _messageType == MessageType.DATA;
    }

    /**
     * returns true if the package contains try flush signal
     * @return true if it is try flush signal
     */
    public boolean isTryFlushSignal() {
        return _messageType == MessageType.TRY_FLUSH_SIGNAL;
    }

    /**
     * returns true if the package contains force flush signal
     * @return true if it is force flush signal
     */
    public boolean isForceFlushSignal() {
        return _messageType == MessageType.FORCE_FLUSH_SIGNAL;
    }

    /**
     * Builder class for {@link Package}
     */
    public static class PackageBuilder {
        private Record _record;
        private String _topic;
        private String _partition;
        private String _offset;
        private long _timestamp;
        private String _destination;
        private SendCallback _ackCallback;
        private String _checkpoint;

        /**
         * Set record
         */
        public PackageBuilder setRecord(Record record) {
            this._record = record;
            return this;
        }

        /**
         * Set source topic
         */
        public PackageBuilder setTopic(String topic) {
            this._topic = topic;
            return this;
        }

        /**
         * Set source partition
         */
        public PackageBuilder setPartition(String partition) {
            this._partition = partition;
            return this;
        }

        /**
         * Set source offset
         */
        public PackageBuilder setOffset(String offset) {
            this._offset = offset;
            return this;
        }

        /**
         * Set source timestamp
         */
        public PackageBuilder setTimestamp(long timestamp) {
            this._timestamp = timestamp;
            return this;
        }

        /**
         * Set destination
         */
        public PackageBuilder setDestination(String destination) {
            this._destination = destination;
            return this;
        }

        /**
         * Set callback to ack
         */
        public PackageBuilder setAckCallBack(SendCallback ackCallback) {
            this._ackCallback = ackCallback;
            return this;
        }

        /**
         * Set checkpoint
         */
        public PackageBuilder setCheckpoint(String checkpoint) {
            this._checkpoint = checkpoint;
            return this;
        }

        /**
         * Build the Package.
         * @return
         *   Package that is created.
         */
        public Package build() {
            return new Package(this);
        }

        /**
         * Build the Package that indicates signal to try flush.
         * @return
         *   Package that is created.
         */
        public Package buildTryFlushSignalPackage() {
            Package pkg = new Package();
            pkg._messageType = MessageType.TRY_FLUSH_SIGNAL;
            return pkg;
        }

        /**
         * Build the Package that indicates signal to force flush.
         * @return
         *   Package that is created.
         */
        public Package buildFroceFlushSignalPackage() {
            Package pkg = new Package();
            pkg._messageType = MessageType.FORCE_FLUSH_SIGNAL;
            return pkg;
        }
    }
}
