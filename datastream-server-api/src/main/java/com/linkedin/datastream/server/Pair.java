package com.linkedin.datastream.server;

import java.util.Objects;


public class Pair<K, V> {

  private final V _value;
  private final K _key;

  public Pair(K key,  V value) {
    _key = key;
    _value = value;
  }

  public V getValue() {
    return _value;
  }

  public K getKey() {
    return _key;
  }

  @SuppressWarnings("unchecked")
  public boolean equals(Object obj)
  {
    if (obj instanceof Pair)
    {
      Pair<K,V> other = (Pair) obj;
      return  Objects.equals(_key, other._key) &&
          Objects.equals(_value, other._value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_key, _value);
  }

  /*convenient method*/
  public static <S, T> Pair<S, T> of(S first, T second)
  {
    return new Pair<>(first, second);
  }
}
