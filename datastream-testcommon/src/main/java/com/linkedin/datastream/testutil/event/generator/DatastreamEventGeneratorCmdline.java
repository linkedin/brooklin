/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatastreamEventGeneratorCmdline {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamEventGeneratorCmdline.class.getName());
  private AtomicReference<Thread> operationRequestThread = new AtomicReference<Thread>(null);
  private boolean isMainThreadDone = true;
  private GlobalSettings globalSettings = new GlobalSettings();

  public DatastreamEventGeneratorCmdline() {
    // nothing for now
  }

  public boolean runWithShutdownHook(String[] args) {
    return run(true, args);
  }

  public boolean run(String[] args) {
    return run(false, args);
  }

  private synchronized boolean run(boolean addShutdownHook, String[] args) {
    Date startTime = new Date();

    if (!globalSettings.parseCommandLineParameters(args)) {
      return false;
    }

    isMainThreadDone = false;

    // start thread that produce data to write/read/delete/update
    startProducerThreads();

    //Add a shutdown hook
    if (addShutdownHook) {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          DatastreamEventGeneratorCmdline.this.stop();
        }
      });
    }
    try {
      waitForProducersToFinish();
    } catch (InterruptedException e) {
      LOG.error("Interrupted with exception ", e);
      isMainThreadDone = true;
      return false;
    }
    Date endTime = new Date();
    long elapsedSeconds = (endTime.getTime() - startTime.getTime()) / 1000;
    LOG.info("Finished: " + elapsedSeconds + " seconds with " + globalSettings.getErrorCount() + " Errors");
    if (globalSettings.getErrorCount() > 0) {
      isMainThreadDone = true;
      return false;
    }
    isMainThreadDone = true;
    return true;
  }

  public void stop() {
    if (isMainThreadDone) {
      return;
    }

    LOG.info("Data generator stop() invoked.");

    // globalSettings.stopProducer = true;

    LOG.info("Waiting for producer and consumer to complete & gracefully exit");
    try {
      waitForProducersToFinish();
    } catch (InterruptedException e) {
      LOG.error("Interrupted with exception ", e);
    }
  }

  private void waitForProducersToFinish() throws InterruptedException {
    Thread producerToStop = operationRequestThread.getAndSet(null);
    if (producerToStop != null) {
      producerToStop.join();
    }
    // globals.isProducerDone = true;
    LOG.info("Put/Delete/Update Request Producer Thread completed producing requests. Waiting for consumers to finish.");
    writeGeneratedIndexToFile();
  }

  private void writeGeneratedIndexToFile() {
    String fileName = "dataGeneratorIndexRange.txt";
    String indexString = String.format("%s - %s", String.valueOf(globalSettings._startResourceKey),
        globalSettings._startResourceKey + globalSettings._numEvents);

    try (BufferedWriter indexWriter = new BufferedWriter(new FileWriter(fileName))) {
      indexWriter.write(indexString);
      indexWriter.flush();
    } catch (IOException e) {
      LOG.error(String.format("Unable to write generated index range to file. Range: %s", indexString), e);
    }
  }

  private void startProducerThreads() {
    Runnable producer = ProducerFactory.getProducer(globalSettings);
    Thread producerThread = new Thread(producer, producer.getClass().getName());

    if (!operationRequestThread.compareAndSet(null, producerThread)) {
      final String error = "Race condition starting producer thread.";
      LOG.error(error);
      return;
    }

    producerThread.start();
  }
}
