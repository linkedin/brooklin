package com.linkedin.datastream.common.zk;

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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Stack;


/**
 * ZKClient is a wrapper of sgroschupf/zkclient. It provides the following
 * basic features:
 * (1) tolerate network reconnects so the caller doesn't have to handle the retries
 * (2) provide a String serializer since we only need to store JSON strings in zookeeper
 * (3) additional features like ensurePath to recursively create paths
 */

public class ZkClient extends org.I0Itec.zkclient.ZkClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZkClient.class);

  public static final String ZK_PATH_SEPARATOR = "/";
  public static final int DEFAULT_CONNECTION_TIMEOUT = 60 * 1000;
  public static final int DEFAULT_SESSION_TIMEOUT = 30 * 1000;
  private ZkSerializer _zkSerializer = new ZKStringSerializer();
  private int _zkSessionTimeoutMs = DEFAULT_SESSION_TIMEOUT;

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout) {
    super(zkServers, sessionTimeout, connectionTimeout, new ZKStringSerializer());

    // TODO: hook this value with configs
    _zkSessionTimeoutMs = sessionTimeout;
  }

  public ZkClient(String zkServers, int connectionTimeout) {
    super(zkServers, DEFAULT_SESSION_TIMEOUT, connectionTimeout, new ZKStringSerializer());
  }

  public ZkClient(String zkServers) {
    super(zkServers, DEFAULT_SESSION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, new ZKStringSerializer());
  }

  @Override
  public void close() throws ZkInterruptedException {
    if (LOG.isTraceEnabled()) {
      StackTraceElement[] calls = Thread.currentThread().getStackTrace();
      LOG.trace("closing a zkclient. callStack: " + Arrays.asList(calls));
    }
    getEventLock().lock();
    try {
      if (_connection == null) {
        return;
      }
      LOG.info("Closing zkclient: " + ((ZkConnection) _connection).getZookeeper());
      super.close();
    } catch (ZkInterruptedException e) {
      /**
       * Workaround for HELIX-264: calling ZkClient#disconnect() in its own eventThread context will
       * throw ZkInterruptedException and skip ZkConnection#disconnect()
       */
      if (_connection != null) {
        try {
          /**
           * ZkInterruptedException#construct() honors InterruptedException by calling
           * Thread.currentThread().interrupt(); clear it first, so we can safely disconnect the
           * zk-connection
           */
          Thread.interrupted();
          _connection.close();
          /**
           * restore interrupted status of current thread
           */
          Thread.currentThread().interrupt();
        } catch (InterruptedException e1) {
          throw new ZkInterruptedException(e1);
        }
      }
    } finally {
      getEventLock().unlock();
      LOG.info("Closed zkclient");
    }
  }

  public Stat getStat(final String path) {
    long startT = System.nanoTime();

    try {
      return retryUntilConnected(() -> ((ZkConnection) _connection).getZookeeper().exists(path, false));
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  // override exists(path, watch), so we can record all exists requests
  @Override
  public boolean exists(final String path, final boolean watch) {
    long startT = System.nanoTime();

    try {
      return retryUntilConnected(() -> _connection.exists(path, watch));
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  // override getChildren(path, watch), so we can record all getChildren requests
  @Override
  public List<String> getChildren(final String path, final boolean watch) {
    long startT = System.nanoTime();

    try {
      return retryUntilConnected(() -> _connection.getChildren(path, watch));
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getChildren, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  /**
   * Read the content of a znode, and make sure to read a valid result. When znodes is in an inconsistent
   * state, for example, being written to by a different process, the ZkClient.readData(path) will return
   * null. This is a helper method to do retry until the value is not null.
   *
   * @param path
   * @return
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
        LOG.error("Failed to sleep at retry: " + counter + " " + e.getMessage());
      }
      totalWait += nextWait;
      content = super.readData(path, false /* returnNullIfPathNotExists */);
    }

    if (content == null) {
      LOG.warn("Failed to read znode data for path " + path + " within timeout " + timeout + " milliseconds");
    }

    return content;
  }

  public String ensureReadData(final String path) {
    return ensureReadData(path, _zkSessionTimeoutMs);
  }

  // override readData(path, stat, watch), so we can record all read requests
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
        LOG.trace("getData, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists) {
    T data = null;
    try {
      data = (T) super.readData(path, stat);
    } catch (ZkNoNodeException e) {
      if (!returnNullIfPathNotExists) {
        throw e;
      }
    }
    return data;
  }

  @Override
  public void writeData(final String path, Object datat, final int expectedVersion) {
    long startT = System.nanoTime();
    try {
      final byte[] data = serialize(datat);

      retryUntilConnected(() -> {
        _connection.writeData(path, data, expectedVersion);
        return null;
      });
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  public Stat writeDataGetStat(final String path, Object datat, final int expectedVersion) throws InterruptedException {
    long start = System.nanoTime();
    try {
      final byte[] bytes = _zkSerializer.serialize(datat);
      return retryUntilConnected(() -> ((ZkConnection) _connection).getZookeeper()
          .setData(path, bytes, expectedVersion));
    } finally {
      long end = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData, path: " + path + ", time: " + (end - start) + " ns");
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
        LOG.trace("create, path: " + path + ", time: " + (endT - startT) + " ns");
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
        LOG.trace("delete, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

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
    while (!this.exists(path)) {
      pstack.push(path);
      path = path.substring(0, path.lastIndexOf(ZK_PATH_SEPARATOR));
      if(path.isEmpty()) {
          path = ZK_PATH_SEPARATOR;
      }
    }

    while (!pstack.empty()) {
      String p = pstack.pop();

      // double check the existence of the path to avoid herd effect
      if (this.exists(p)) {
        continue;
      }

      LOG.info("creating path in zookeeper: " + p);
      try {
        this.createPersistent(p);
      } catch (ZkNodeExistsException e) {
        LOG.info(e.getMessage());
      }
    }
  }

  public byte[] serialize(Object data) {
    return _zkSerializer.serialize(data);
  }

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
      try {
        ret = ((String) data).getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        LOG.error(e.getMessage());
      }
      return ret;
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      String data = null;
      if (bytes != null) {
        try {
          data = new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
          LOG.error(e.getMessage());
        }
      }
      return data;
    }
  }

}
