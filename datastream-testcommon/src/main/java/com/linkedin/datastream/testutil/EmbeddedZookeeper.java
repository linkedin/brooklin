/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.Validate;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.FileUtils;


/**
 * Encapsulates a simple standalone ZooKeeper server
 */
public class EmbeddedZookeeper {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedZookeeper.class);

  private int _port = -1;
  private int _tickTime = 500;

  private ServerCnxnFactory _factory;
  private File _snapshotDir;
  private String _snapshotDirPath;
  private File _logDir;
  private String _logDirPath;

  private boolean _started;

  public ZooKeeperServer getZooKeeperServer() {
    return _zooKeeperServer;
  }

  private ZooKeeperServer _zooKeeperServer;

  /**
   * Construct an EmbeddedZookeeper bound to a random port
   */
  public EmbeddedZookeeper() throws IOException {
    this(0);
  }

  /**
   * Construct an EmbeddedZookeeper bound to the specified {@code port}
   */
  public EmbeddedZookeeper(int port) throws IOException {
    this(port, 500);
  }

  /**
   * Construct an EmbeddedZookeeper
   * @param port the port to bind to
   * @param tickTime ZooKeeper tick time, used to regulate heartbeats and timeouts
   */
  public EmbeddedZookeeper(int port, int tickTime) throws IOException {
    this._factory = NIOServerCnxnFactory.createFactory(port, 1024);
    this._port = _factory.getLocalPort();
    this._tickTime = tickTime;
  }

  /**
   * Construct an EmbeddedZookeeper
   * @param port the port to bind to
   * @param snapshotDirPath snapshot directory path
   * @param logDirPath log directory path
   */
  public EmbeddedZookeeper(int port, String snapshotDirPath, String logDirPath) throws IOException {
    this(port);
    this._snapshotDirPath = snapshotDirPath;
    this._logDirPath = logDirPath;
  }

  /**
   * Start up EmbeddedZookeeper
   */
  public void startup() throws IOException {
    Validate.isTrue(this._port > 0, "Failed to reserve port for zookeeper server.");
    LOG.info("Starting Zookeeper Cluster");
    if (this._snapshotDirPath == null) {
      this._snapshotDir = FileUtils.constructRandomDirectoryInTempDir("embedded-zk/snapshot-" + this._port);
    } else {
      this._snapshotDir = FileUtils.constructDirectoryInTempDir("embedded-zk/snapshot-" + this._port);
    }

    if (this._logDirPath == null) {
      this._logDir = FileUtils.constructRandomDirectoryInTempDir("embedded-zk/log-" + this._port);
    } else {
      this._logDir = FileUtils.constructDirectoryInTempDir("embedded-zk/log-" + this._port);
    }

    try {
      _zooKeeperServer = new ZooKeeperServer(this._snapshotDir, this._logDir, this._tickTime);
      _factory.startup(_zooKeeperServer);
      _started = true;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    LOG.info("Zookeeper started with ..." +
        "\n  Port: " + this._port +
        "\n  Snapshot Dir Path: " + this._snapshotDirPath +
        "\n  Log Dir Path: " + this._logDirPath);
  }

  /**
   * Shut down EmbeddedZookeeper
   */
  public void shutdown() {
    if (!_started) {
      return;
    }

    _factory.shutdown();
    try {
      FileUtils.deleteFile(_snapshotDir);
    } catch (FileNotFoundException e) {
      // ignore
    }
    try {
      FileUtils.deleteFile(_logDir);
    } catch (FileNotFoundException e) {
      // ignore
    }
    _started = false;
    _zooKeeperServer = null;
  }

  /**
   * Get connection URI
   */
  public String getConnection() {
    return "localhost:" + _port;
  }

  public int getPort() {
    return _port;
  }

  public void setPort(int port) {
    this._port = port;
  }

  public int getTickTime() {
    return _tickTime;
  }

  public void setTickTime(int tickTime) {
    this._tickTime = tickTime;
  }

  /**
   * Return true if EmbeddedZookeeper has been started up
   * @see #startup()
   */
  public boolean isStarted() {
    return _started;
  }

  public String getSnapshotDirPath() {
    return _snapshotDirPath;
  }

  public String getLogDirPath() {
    return _logDirPath;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("EmbeddedZookeeper{");
    sb.append("connection=").append(getConnection());
    sb.append('}');
    return sb.toString();
  }

  private static CommandLine parseArgs(String[] args) {
    Options options = new Options();

    options.addOption("p", "port", true, "Zookeeper port number to use");
    options.addOption("l", "logDir", true, "Zookeeper logDir");
    options.addOption("s", "snapshotDir", true, "Zookeeper snapshotDir");

    // Parse the command line options
    CommandLineParser parser = new BasicParser();
    CommandLine commandLine;
    try {
      commandLine = parser.parse(options, args);
    } catch (Exception e) {
      commandLine = null;
      LOG.error(e.getMessage());
    }
    return commandLine;
  }

  /**
   * Main entry point for starting an EmbeddedZookeeper from command line.
   */
  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseArgs(args);
    if (commandLine != null) {
      int port = 0;
      String snapshotDirPath = null;
      String logDirPath = null;

      if (commandLine.hasOption("p")) {
        port = Integer.parseInt(commandLine.getOptionValue("p"));
      }
      if (commandLine.hasOption("l")) {
        logDirPath = commandLine.getOptionValue("l");
      }
      if (commandLine.hasOption("s")) {
        snapshotDirPath = commandLine.getOptionValue("s");
      }

      EmbeddedZookeeper zk = new EmbeddedZookeeper(port, snapshotDirPath, logDirPath);
      zk.startup();
    }
  }
}
