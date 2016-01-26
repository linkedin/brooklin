package com.linkedin.datastream.common;

import java.io.IOException;
import java.net.ServerSocket;


/**
 * Class that contains the helper utility methods for Network operations.
 */
public class NetworkUtils {

  /**
   * @return
   *  The next available port that can be used for opening a socket connection.
   */
  public synchronized static int getAvailablePort() {
    try {
      ServerSocket socket = new ServerSocket(0);
      try {
        socket.setReuseAddress(true);
        return socket.getLocalPort();
      } finally {
        socket.close();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Cannot find available port: " + e.getMessage(), e);
    }
  }
}
