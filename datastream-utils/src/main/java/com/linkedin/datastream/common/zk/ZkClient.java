/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.zk;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Stack;

import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ZKClient is a wrapper of {@link org.I0Itec.zkclient.ZkClient}. It provides the following
 * basic features:
 * <ol>
 *  <li>tolerate network reconnects so the caller doesn't have to handle the retries</li>
 *  <li>provide a String serializer since we only need to store JSON strings in ZooKeeper</li>
 *  <li>additional features like ensurePath to recursively create paths</li>
 * </ol>
 */
public class ZkClient extends org.I0Itec.zkclient.ZkClient {
  public static final String ZK_PATH_SEPARATOR = "/";
  public static final int DEFAULT_CONNECTION_TIMEOUT = 60 * 1000;
  public static final int DEFAULT_SESSION_TIMEOUT = 30 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(ZkClient.class);

  private final ZkSerializer _zkSerializer = new ZKStringSerializer();
  private int _zkSessionTimeoutMs = DEFAULT_SESSION_TIMEOUT;

  /**
   * Constructor for ZkClient
   * @param zkServers the ZooKeeper connection String
   * @param sessionTimeout the session timeout in milliseconds
   * @param connectionTimeout the connection timeout in milliseconds
   */
  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout) {
    super(zkServers, sessionTimeout, connectionTimeout, new ZKStringSerializer());

    _zkSessionTimeoutMs = sessionTimeout;
  }

  /**
   * Constructor for ZkClient
   * @param zkServers the ZooKeeper connection String
   * @param connectionTimeout the connection timeout in milliseconds
   */
  public ZkClient(String zkServers, int connectionTimeout) {
    super(zkServers, DEFAULT_SESSION_TIMEOUT, connectionTimeout, new ZKStringSerializer());
  }

  /**
   * Constructor for ZkClient
   * @param zkServers the ZooKeeper connection String
   */
  public ZkClient(String zkServers) {
    super(zkServers, DEFAULT_SESSION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, new ZKStringSerializer());
  }

  @Override
  public void close() throws ZkInterruptedException {
    if (LOG.isTraceEnabled()) {
      StackTraceElement[] calls = Thread.currentThread().getStackTrace();
      LOG.trace("closing zkclient. callStack: {}", Arrays.asList(calls));
    }
    getEventLock().lock();
    try {
      if (_connection == null) {
        return;
      }
      LOG.info("closing zkclient: {}", ((ZkConnection) _connection).getZookeeper());
      super.close();
    } catch (ZkInterruptedException e) {
      /*
       * Workaround for HELIX-264: calling ZkClient#disconnect() in its own eventThread context will
       * throw ZkInterruptedException and skip ZkConnection#disconnect()
       */
      try {
        /*
         * ZkInterruptedException#construct() honors InterruptedException by calling
         * Thread.currentThread().interrupt(); clear it first, so we can safely disconnect the
         * zk-connection
         */
        Thread.interrupted();
        _connection.close();
        /*
         * restore interrupted status of current thread
         */
        Thread.currentThread().interrupt();
      } catch (InterruptedException e1) {
        throw new ZkInterruptedException(e1);
      }
    } finally {
      getEventLock().unlock();
      LOG.info("closed zkclient");
    }
  }

  @Override
  public boolean exists(final String path, final boolean watch) {
    long startT = System.nanoTime();

    try {
      return retryUntilConnected(() -> _connection.exists(path, watch));
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: {}, time: {} ns", path, (endT - startT));
      }
    }
  }

  @Override
  public List<String> getChildren(final String path, final boolean watch) {
    long startT = System.nanoTime();

    try {
      return retryUntilConnected(() -> _connection.getChildren(path, watch));
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getChildren, path: {}, time: {} ns", path, (endT - startT));
      }
    }
  }

  /**
   * Read the content of a znode and make sure to read a valid result. When znode is in an inconsistent
   * state (e.g. being written to by a different process), the ZkClient.readData(path) will return
   * null. This is a helper method to retry until the return value is non-null or until {@code timeout} is reached
   * @param path the path of the znode to read
   * @param timeout the timeout, in milliseonds, after which this method will return even if the value is null
   * @return the content of the znode at the given path
   */
  public String ensureReadData(final String path, final long timeout) {
    Random rn = new Random();
    int jitter;
    int retry = 1;
    int step = 50;
    int counter = 0;

    String content = super.readData(path, false);

    long totalWait = 0;
    long nextWait;

    while (content == null && totalWait < timeout) {
      counter++;
      retry *= 2;
      jitter = rn.nextInt(100);

      // calculate the next waiting time, and make sure the total
      // wait will never pass the specified timeout value.
      nextWait = retry * step + jitter;
      if (totalWait + nextWait > timeout) {
        nextWait = timeout - totalWait;
      }

      try {
        Thread.sleep(nextWait);
      } catch (InterruptedException e) {
        LOG.error("Failed to sleep at retry: {}", counter, e);
      }
      totalWait += nextWait;
      content = super.readData(path, false);
    }

    if (content == null) {
      LOG.warn("Failed to read znode data for path {} within timeout of {} milliseconds", path, timeout);
    }

    return content;
  }

  /**
   * Read the content of a znode and make sure to read a valid result. When znode is in an inconsistent state (e.g.
   * being written to by a different process), the ZkClient.readData(path) will return null. This is a helper method to
   * retry until the return value is non-null or until the zk session timeout is reached
   * @param path the path of the znode to read
   * @return the content of the znode at the given path
   */
  public String ensureReadData(final String path) {
    return ensureReadData(path, _zkSessionTimeoutMs);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <T extends Object> T readData(final String path, final Stat stat, final boolean watch) {
    long startT = System.nanoTime();
    try {
      byte[] data = retryUntilConnected(() -> _connection.readData(path, stat, watch));
      return (T) deserialize(data);
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("readData, path: {}, time: {} ns", path, (endT - startT));
      }
    }
  }

  @Override
  public void writeData(final String path, Object data, final int expectedVersion) {
    long startT = System.nanoTime();
    try {
      final byte[] bytes = serialize(data);

      retryUntilConnected(() -> {
        _connection.writeData(path, bytes, expectedVersion);
        return null;
      });
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("writeData, path: {}, time: {} ns", path, (endT - startT));
      }
    }
  }

  @Override
  public String create(final String path, Object data, final CreateMode mode) throws RuntimeException {
    if (path == null) {
      throw new IllegalArgumentException("path must not be null.");
    }

    long startT = System.nanoTime();
    try {
      final byte[] bytes = data == null ? null : serialize(data);

      return retryUntilConnected(() -> _connection.create(path, bytes, mode));
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("create, path: {}, time: {} ns", path, (endT - startT));
      }
    }
  }

  @Override
  public boolean delete(final String path) {
    long startT = System.nanoTime();
    try {
      try {
        retryUntilConnected(() -> {
          _connection.delete(path);
          return null;
        });

        return true;
      } catch (ZkNoNodeException e) {
        return false;
      }
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("delete, path: {}, time: {} ns", path, (endT - startT));
      }
    }
  }

  /**
   * Ensure that all the paths in the given full path String are created
   * @param path the zk path
   */
  public void ensurePath(String path) {
    if (path == null) {
      return;
    }

    // if the path exists, return
    if (this.exists(path)) {
      return;
    }

    // paths to create in the stack
    Stack<String> pstack = new Stack<>();

    // push paths in stack because we need to create from parent to children
    while (!exists(path)) {
      pstack.push(path);
      path = path.substring(0, path.lastIndexOf(ZK_PATH_SEPARATOR));
      if (path.isEmpty()) {
        path = ZK_PATH_SEPARATOR;
      }
    }

    while (!pstack.empty()) {
      String p = pstack.pop();

      // double check the existence of the path to avoid herd effect
      if (exists(p)) {
        continue;
      }

      LOG.info("creating path in zookeeper: {}", p);
      try {
        this.createPersistent(p);
      } catch (ZkNodeExistsException e) {
        LOG.info(e.getMessage());
      }
    }
  }

  /**
   * Serialize the given data into a byte array using the ZkSerializer
   */
  public byte[] serialize(Object data) {
    return _zkSerializer.serialize(data);
  }

  /**
   * Deserialize the given data using the ZkSerializer
   */
  @SuppressWarnings("unchecked")
  public <T extends Object> T deserialize(byte[] data) {
    if (data == null) {
      return null;
    }
    return (T) _zkSerializer.deserialize(data);
  }

  private static class ZKStringSerializer implements ZkSerializer {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
      byte[] ret = null;
      ret = ((String) data).getBytes(StandardCharsets.UTF_8);
      return ret;
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      String data = null;
      if (bytes != null) {
        data = new String(bytes, StandardCharsets.UTF_8);
      }
      return data;
    }
  }
}
