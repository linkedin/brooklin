package com.linkedin.datastream.testutil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.linkedin.datastream.common.FileUtils;
import com.linkedin.datastream.common.NetworkUtils;


public class EmbeddedZookeeper {
  private int port = -1;
  private int tickTime = 500;

  private ServerCnxnFactory factory;
  private File snapshotDir;
  private File logDir;

  private boolean _started;

  public EmbeddedZookeeper() {
    this(-1);
  }

  public EmbeddedZookeeper(int port) {
    this(port, 500);
  }

  public EmbeddedZookeeper(int port, int tickTime) {
    this.port = resolvePort(port);
    this.tickTime = tickTime;
  }

  private int resolvePort(int port) {
    if (port == -1) {
      return NetworkUtils.getAvailablePort();
    }
    return port;
  }

  public void startup() throws IOException {
    if (this.port == -1) {
      this.port = NetworkUtils.getAvailablePort();
    }
    this.factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 1024);
    this.snapshotDir = FileUtils.constructRandomDirectoryInTempDir("embedded-zk/snapshot-" + this.port);
    this.logDir = FileUtils.constructRandomDirectoryInTempDir("embedded-zk/log-" + this.port);

    try {
      factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime));
      _started = true;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public void shutdown() {
    if (!_started) {
      return;
    }

    factory.shutdown();
    try {
      FileUtils.deleteFile(snapshotDir);
    } catch (FileNotFoundException e) {
      // ignore
    }
    try {
      FileUtils.deleteFile(logDir);
    } catch (FileNotFoundException e) {
      // ignore
    }
    _started = false;
  }

  public String getConnection() {
    return "localhost:" + port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setTickTime(int tickTime) {
    this.tickTime = tickTime;
  }

  public int getPort() {
    return port;
  }

  public int getTickTime() {
    return tickTime;
  }

  public boolean isStarted() {
    return _started;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("EmbeddedZookeeper{");
    sb.append("connection=").append(getConnection());
    sb.append('}');
    return sb.toString();
  }
}
