/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Objects;


/**
 * Key-value pair
 * @param <K> type of key
 * @param <V> type of value
 */
public class Pair<K, V> {

  private final V _value;
  private final K _key;

  /**
   * Constructor using key and value
   */
  public Pair(K key, V value) {
    _key = key;
    _value = value;
  }

  public V getValue() {
    return _value;
  }

  public K getKey() {
    return _key;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object obj) {
    if (obj instanceof Pair) {
      Pair<K, V> other = (Pair) obj;
      return Objects.equals(_key, other._key) && Objects.equals(_value, other._value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_key, _value);
  }

  /**
   * Convenience method for creating a pair given a key and a value
   */
  public static <S, T> Pair<S, T> of(S first, T second) {
    return new Pair<>(first, second);
  }
}
