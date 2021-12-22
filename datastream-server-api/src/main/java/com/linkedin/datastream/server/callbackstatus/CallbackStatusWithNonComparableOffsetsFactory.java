/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.callbackstatus;

/**
 * Factory implementation for Callback Status With Non Comparable Offsets
 * @param <T> The type of the offset position that the underlying pub-sub system uses.
 */
public class CallbackStatusWithNonComparableOffsetsFactory<T extends Comparable<T>> implements CallbackStatusFactory<T> {

  /**
   * Creates a callback status strategy that checkpoints the consumer offset on successful produce of that record
   * with non comparable offsets
   * @return CallbackStatus strategy construct
   */
  @Override
  public CallbackStatus<T> createCallbackStatusStrategy() {
    return new CallbackStatusWithNonComparableOffsets<T>();
  }
}
