package com.linkedin.datastream.server.zk;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.Stack;

import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZKClient is a wrapper of sgroschupf/zkclient. It provides the following
 * basic features:
 * (1) tolerate network reconnects so the caller doesn't have to handle the retries
 * (2) provide a String serializer since we only need to store JSON strings in zookeeper
 * (3) additional features like ensurePath to recursively create paths
 */

public class ZkClient extends org.I0Itec.zkclient.ZkClient {
  private static Logger LOG = LoggerFactory.getLogger(ZkClient.class);
  public static final int DEFAULT_CONNECTION_TIMEOUT = 60 * 1000;
  public static final int DEFAULT_SESSION_TIMEOUT = 30 * 1000;
  private ZkSerializer _zkSerializer = new ZKStringSerializer();

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout) {
    super(zkServers, sessionTimeout, connectionTimeout, new ZKStringSerializer());
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
          _connection = null;
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
      Stat stat = retryUntilConnected(new Callable<Stat>() {

        @Override
        public Stat call() throws Exception {
          Stat stat = ((ZkConnection) _connection).getZookeeper().exists(path, false);
          return stat;
        }
      });

      return stat;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  // override exists(path, watch), so we can record all exists requests
  @Override
  protected boolean exists(final String path, final boolean watch) {
    long startT = System.nanoTime();

    try {
      return retryUntilConnected(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return _connection.exists(path, watch);
        }
      });
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  // override getChildren(path, watch), so we can record all getChildren requests
  @Override
  protected List<String> getChildren(final String path, final boolean watch) {
    long startT = System.nanoTime();

    try {
      return retryUntilConnected(new Callable<List<String>>() {
        @Override
        public List<String> call() throws Exception {
          return _connection.getChildren(path, watch);
        }
      });
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getChildren, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  // override readData(path, stat, watch), so we can record all read requests
  @Override
  @SuppressWarnings("unchecked")
  protected <T extends Object> T readData(final String path, final Stat stat, final boolean watch) {
    long startT = System.nanoTime();
    try {
      byte[] data = retryUntilConnected(new Callable<byte[]>() {

        @Override
        public byte[] call() throws Exception {
          return _connection.readData(path, stat, watch);
        }
      });
      return (T) deserialize(data);
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getData, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }  

  @SuppressWarnings("unchecked")
  public <T extends Object> T readDataAndStat(String path, Stat stat,
      boolean returnNullIfPathNotExists) {
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

      retryUntilConnected(new Callable<Object>() {

        @Override
        public Object call() throws Exception {
          _connection.writeData(path, data, expectedVersion);
          return null;
        }
      });
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  public Stat writeDataGetStat(final String path, Object datat, final int expectedVersion)
      throws InterruptedException {
    long start = System.nanoTime();
    try {
      final byte[] bytes = _zkSerializer.serialize(datat);
      return retryUntilConnected(new Callable<Stat>() {

        @Override
        public Stat call() throws Exception {
          return ((ZkConnection) _connection).getZookeeper().setData(path, bytes, expectedVersion);
        }
      });
    } finally {
      long end = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData, path: " + path + ", time: " + (end - start) + " ns");
      }
    }
  }  

  @Override
  public String create(final String path, Object data, final CreateMode mode)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    if (path == null) {
      throw new NullPointerException("path must not be null.");
    }

    long startT = System.nanoTime();
    try {
      final byte[] bytes = data == null ? null : serialize(data);

      return retryUntilConnected(new Callable<String>() {

        @Override
        public String call() throws Exception {
          return _connection.create(path, bytes, mode);
        }
      });
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
        retryUntilConnected(new Callable<Object>() {

          @Override
          public Object call() throws Exception {
            _connection.delete(path);
            return null;
          }
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
      File f = new File(path);
      path = f.getParent();
    }

    while (!pstack.empty()) {
      String p = pstack.pop();
      LOG.info("creating path in zookeeper: " + p);
      this.createPersistent(p);
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
        ret = ((String)data).getBytes("UTF-8");
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
