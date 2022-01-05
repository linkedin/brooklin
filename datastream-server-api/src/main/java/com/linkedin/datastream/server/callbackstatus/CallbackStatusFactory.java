/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.callbackstatus;

/**
 * Interface for CallbackStatus Factories
 * @param <T> The type of the offset position that the underlying pub-sub system uses.
 */
public interface CallbackStatusFactory<T> {
  /**
   * Creates a callback status strategy that checkpoints the consumer offset on successful produce of that record
   * @return CallbackStatus strategy construct
   */
  CallbackStatus<T> createCallbackStatusStrategy();
}