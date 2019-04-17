/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.io.IOException;
import java.net.ServerSocket;


/**
 * Class that contains the helper utility methods for Network operations.
 */
public class NetworkUtils {

  /**
   * Get the next available port that can be used for opening a socket connection.
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
