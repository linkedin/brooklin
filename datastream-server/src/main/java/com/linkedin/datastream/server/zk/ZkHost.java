/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.zk;

import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZkHost provide the last register time of particular host. It allows unstable hosts, which join and leave in a short
 * period of time to be blacklisted, and avoid them from participating the task assignment
 */
public class ZkHost {
  private static final Logger LOG = LoggerFactory.getLogger(ZkHost.class);
  private String _hostname;
  private String _instanceName;
  private long _blacklistedTime;
  private long _blacklistedDurationMs;
  private long _windowLengthMs;
  private int _maxJoinAllowed;
  //Maintain a sorted register time, with smallest in the front
  private Queue<Long> _registerTime = new PriorityBlockingQueue<>();

  public String getHostname() {
    return _hostname;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  /**
   * Construct a ZKHost instance
   * @param instanceName the instance name from Zookeepr
   * @param maxJoinAllowed
   * @param blacklistedDurationMs
   * @param windowLengthMs
   */
  public ZkHost(String instanceName, int maxJoinAllowed, long blacklistedDurationMs, long windowLengthMs) {
    _hostname = instanceName.substring(0, instanceName.lastIndexOf("-"));
    _instanceName = instanceName;
    _registerTime.add(System.currentTimeMillis());
    _blacklistedTime = 0L;
    _blacklistedDurationMs = blacklistedDurationMs;
    _windowLengthMs = windowLengthMs;
    _maxJoinAllowed = maxJoinAllowed;
  }


  /**
   * register the join action for a particular instance, it will trigger the blacklist if there is multiple join within
   * a small number of duration
   * @param instanceName
   */
  public void registerJoin(String instanceName) throws IllegalArgumentException {
    // do not record this join if the entire instance name is the same
    if (instanceName.equals(_instanceName)) {
      return;
    }

    String hostname = instanceName.substring(0, instanceName.lastIndexOf("-"));
    if (!_hostname.equals(hostname)) {
      throw new IllegalArgumentException("instance " + instanceName + " doesnt belong to the host " + _hostname);
    }

    long now = System.currentTimeMillis();


    if (isBlacklisted()) {
      //clean blacklist status if its larger than the window
      if ((now - _blacklistedTime) > _blacklistedDurationMs) {
        _registerTime.clear();
        _blacklistedTime = 0L;
      } else {
        LOG.warn("instance {} has been blacklist since {}, ignore the join", _instanceName, _blacklistedTime);
        return;
      }
    }

    //update registration
    _registerTime.add(now);
    _instanceName = instanceName;

    do {
      if (_registerTime.peek() < (now - _windowLengthMs)) {
        _registerTime.remove();
      } else {
        break;
      }
    } while (!_registerTime.isEmpty());

    if (_registerTime.size() > _maxJoinAllowed) {
      LOG.warn("instance {}: is blacklisted since {}", _instanceName, _blacklistedTime);
      _blacklistedTime = now;
    }

  }

  public boolean isBlacklisted() {
    return _blacklistedTime > 0;
  }
}
