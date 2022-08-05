/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.zk;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.Stack;

import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ZKClient is a wrapper of {@link org.apache.helix.zookeeper.impl.client.ZkClient}. It provides the following
 * basic features:
 * <ol>
 *  <li>tolerate network reconnects so the caller doesn't have to handle the retries</li>
 *  <li>provide a String serializer since we only need to store JSON strings in ZooKeeper</li>
 *  <li>additional features like ensurePath to recursively create paths</li>
 * </ol>
 */
public class ZkClient extends org.apache.helix.zookeeper.impl.client.ZkClient {
  public static final String ZK_PATH_SEPARATOR = "/";
  public static final int DEFAULT_CONNECTION_TIMEOUT = 60 * 1000;
  public static final int DEFAULT_SESSION_TIMEOUT = 30 * 1000;
  public static final int DEFAULT_OPERATION_RETRY_TIMEOUT = -1;

  private static final Logger LOG = LoggerFactory.getLogger(ZkClient.class);

  private final ZkSerializer _zkSerializer = new ZKStringSerializer();
  private int _zkSessionTimeoutMs;

  /**
   * Constructor for ZkClient
   * @param zkServers the ZooKeeper connection String
   */
  public ZkClient(String zkServers) {
    this(zkServers, DEFAULT_SESSION_TIMEOUT);
  }

  /**
   * Constructor for ZkClient
   * @param zkServers the ZooKeeper connection String
   * @param sessionTimeoutMs the session timeout in milliseconds
   */
  public ZkClient(String zkServers, int sessionTimeoutMs) {
    this(zkServers, sessionTimeoutMs, DEFAULT_CONNECTION_TIMEOUT);
  }

  /**
   * Constructor for ZkClient
   * @param zkServers the ZooKeeper connection String
   * @param sessionTimeoutMs the session timeout in milliseconds
   * @param connectionTimeoutMs the connection timeout in milliseconds
   */
  public ZkClient(String zkServers, int sessionTimeoutMs, int connectionTimeoutMs) {
    this(zkServers, sessionTimeoutMs, connectionTimeoutMs, DEFAULT_OPERATION_RETRY_TIMEOUT);
  }

  /**
   * Constructor for ZkClient
   * @param zkServers the ZooKeeper connection String
   * @param sessionTimeoutMs the session timeout in milliseconds
   * @param connectionTimeoutMs the connection timeout in milliseconds
   * @param operationRetryTimeoutMs The maximum amount of time, in milli seconds, each failed
   *                                operation is retried. A value lesser than 0 is considered as
   *                                retry forever until a connection has been reestablished.
   */
  public ZkClient(String zkServers, int sessionTimeoutMs, int connectionTimeoutMs, int operationRetryTimeoutMs) {
    super(zkServers, sessionTimeoutMs, connectionTimeoutMs, new ZKStringSerializer(), operationRetryTimeoutMs);
    _zkSessionTimeoutMs = sessionTimeoutMs;
  }

  /**
   * Check if a zk path exists. Changes the access modified to public, its defined as protected in parent class.
   */
  @Override
  public boolean exists(final String path, final boolean watch) {
    return super.exists(path, watch);
  }

  /**
   * Get all children of zk path. Changes the access modified to public, its defined as protected in parent class.
   */
  @Override
  public List<String> getChildren(final String path, final boolean watch) {
    return super.getChildren(path, watch);
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
