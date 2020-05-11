/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.cloud.storage.committer.ObjectCommitter;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.VerifiableProperties;

import com.linkedin.datastream.server.DatastreamProducerRecord;

import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;

/**
 * This is a Cloud Storage Transport provider that writes events to specified cloud Storage.
 * It handles writing records to specified file formats and committing the files to the specified cloud storage.
 */
public class CloudStorageTransportProvider implements TransportProvider {

   private static final Logger LOG = LoggerFactory.getLogger(CloudStorageTransportProvider.class.getName());

   private static final String KAFKA_ORIGIN_TOPIC = "kafka-origin-topic";
   private static final String KAFKA_ORIGIN_PARTITION = "kafka-origin-partition";
   private static final String KAFKA_ORIGIN_OFFSET = "kafka-origin-offset";

   private String _transportProviderName;
   private final ScheduledExecutorService _scheduler = new ScheduledThreadPoolExecutor(1);
   private CopyOnWriteArrayList<ObjectBuilder> _objectBuilders = new CopyOnWriteArrayList<>();
   private final ObjectCommitter _committer;

   private volatile boolean _isClosed;

   private static void initializeLocalDirectory(String localDirectory) {
      java.io.File localDir = new java.io.File(localDirectory);
      if (!localDir.exists()) {
         LOG.info("Creating directory {}.", localDirectory);
         if (!localDir.mkdirs()) {
            LOG.error("Unable to create the IO directory {}", localDir);
            throw new RuntimeException("Unable to create the IO directory");
         }
      }

      try {
         LOG.info("Cleaning the directory {}.", localDirectory);
         FileUtils.cleanDirectory(localDir);
      } catch (IOException e) {
         LOG.error("Unable to clear the IO directory {}", localDir);
         throw new RuntimeException(e);
      }
   }

   private CloudStorageTransportProvider(CloudStorageTransportProviderBuilder builder) {

      this._isClosed = false;
      this._transportProviderName = builder._trasnportProviderName;
      this._committer = builder._committer;

      initializeLocalDirectory(builder._localDirectory);

      // initialize and start object builders
      for (int i = 0; i < builder._objectBuilderCount; i++) {
         _objectBuilders.add(new ObjectBuilder(
                 builder._ioClass,
                 builder._ioProperties,
                 builder._localDirectory,
                 builder._maxFileSize,
                 builder._maxFileAge,
                 builder._maxInflightWriteLogCommits,
                 builder._committer,
                 builder._objectBuilderQueueSize));
      }
      for (ObjectBuilder objectBuilder : _objectBuilders) {
          objectBuilder.start();
      }

      // send periodic flush signal to commit stale objects
      _scheduler.scheduleAtFixedRate(
              () -> {
                 for (ObjectBuilder objectBuilder: _objectBuilders) {
                    LOG.info("Try flush signal sent.");
                    objectBuilder.assign(new Package.PackageBuilder().buildTryFlushSignalPackage());
                 }
                 },
              builder._maxFileAge / 2,
              builder._maxFileAge / 2,
              TimeUnit.MILLISECONDS);
   }

   private void delegate(final Package aPackage) {
      this._objectBuilders.get(Math.abs(aPackage.hashCode() % _objectBuilders.size())).assign(aPackage);
   }

   @Override
   public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
      for (final BrooklinEnvelope env :  record.getEvents()) {
         final Package aPackage = new Package.PackageBuilder()
                 .setRecord(new Record(env.getKey(), env.getValue()))
                 .setTopic(env.getMetadata().get(KAFKA_ORIGIN_TOPIC))
                 .setPartition(env.getMetadata().get(KAFKA_ORIGIN_PARTITION))
                 .setOffset(env.getMetadata().get(KAFKA_ORIGIN_OFFSET))
                 .setTimestamp(env.getMetadata().get(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP))
                 .setDestination(destination)
                 .setAckCallBack(onComplete)
                 .setCheckpoint(record.getCheckpoint())
                 .build();
         delegate(aPackage);
      }
   }

   @Override
   public synchronized void close() {
      if (_isClosed) {
        LOG.info("Transport provider {} is already closed.", _transportProviderName);
        return;
      }

      try {
         LOG.info("Closing the transport provider {}", _transportProviderName);
         for (ObjectBuilder objectBuilder : _objectBuilders) {
            objectBuilder.shutdown();
         }

         for (ObjectBuilder objectBuilder : _objectBuilders) {
            try {
               objectBuilder.join();
            } catch (InterruptedException e) {
               LOG.warn("An interrupt was raised during join() call on a Object Builder");
               Thread.currentThread().interrupt();
            }
         }

         _committer.shutdown();
      } finally {
         _isClosed = true;
      }
   }

   @Override
   public void flush() {
      LOG.info("Forcing flush on object builders.");
      List<Package> flushSignalPackages = new ArrayList<>();
      for (final ObjectBuilder objectBuilder : _objectBuilders) {
         final Package aPackage = new Package.PackageBuilder().buildFroceFlushSignalPackage();
         flushSignalPackages.add(aPackage);
         objectBuilder.assign(aPackage);
      }
      for (final Package aPackage : flushSignalPackages) {
         aPackage.waitUntilDelivered();
      }
   }

   /**
    * Builder class for {@link com.linkedin.datastream.cloud.storage.CloudStorageTransportProvider}
    */
   public static class CloudStorageTransportProviderBuilder {
      private String _trasnportProviderName;
      private int _objectBuilderQueueSize;
      private int _objectBuilderCount;
      private String _localDirectory;
      private long _maxFileSize;
      private int _maxFileAge;
      private int _maxInflightWriteLogCommits;
      private ObjectCommitter _committer;
      private String _ioClass;
      private VerifiableProperties _ioProperties;

      /**
       * Set the name of the transport provider
       */
      public CloudStorageTransportProviderBuilder setTransportProviderName(String trasnportProviderName) {
         this._trasnportProviderName = trasnportProviderName;
         return this;
      }

      /**
       * Set object builder's queue size
       */
      public CloudStorageTransportProviderBuilder setObjectBuilderQueueSize(int objectBuilderQueueSize) {
         this._objectBuilderQueueSize = objectBuilderQueueSize;
         return this;
      }

      /**
       * Set number of object builders
       */
      public CloudStorageTransportProviderBuilder setObjectBuilderCount(int objectBuilderCount) {
         this._objectBuilderCount = objectBuilderCount;
         return this;
      }

      /**
       * Set local directory when objects are stored temporarily before committing to cloud storage
       */
      public CloudStorageTransportProviderBuilder setLocalDirectory(String localDirectory) {
         this._localDirectory = localDirectory;
         return this;
      }

      /**
       * Set max file size of the object
       */
      public CloudStorageTransportProviderBuilder setMaxFileSize(long maxFileSize) {
         this._maxFileSize = maxFileSize;
         return this;
      }

      /**
       * Set max file age of the object
       */
      public CloudStorageTransportProviderBuilder setMaxFileAge(int maxFileAge) {
         this._maxFileAge = maxFileAge;
         return this;
      }

      /**
       * Set max commit backlog size
       */
      public CloudStorageTransportProviderBuilder setMaxInflightWriteLogCommits(int maxInflightWriteLogCommits) {
         this._maxInflightWriteLogCommits = maxInflightWriteLogCommits;
         return this;
      }

      /**
       * Set object committer
       */
      public CloudStorageTransportProviderBuilder setObjectCommitter(ObjectCommitter committer) {
         this._committer = committer;
         return this;
      }

      /**
       * Set IO class
       */
      public CloudStorageTransportProviderBuilder setIOClass(String ioClass) {
         this._ioClass = ioClass;
         return this;
      }

      /**
       * Set configuration options for IO
       */
      public CloudStorageTransportProviderBuilder setIOProperties(VerifiableProperties ioProperties) {
         this._ioProperties = ioProperties;
         return this;
      }

      /**
       * Build the CloudStorageTransportProvider.
       * @return
       *   CloudStorageTransportProvider that is created.
       */
      public CloudStorageTransportProvider build() {
         return new CloudStorageTransportProvider(this);
      }
   }
}
