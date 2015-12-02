package com.linkedin.datastream.common;


import java.lang.reflect.Field;
import java.util.Objects;
import java.util.stream.IntStream;
import java.lang.reflect.Constructor;

/**
 * Utility class to simplify usage of Java reflection.
 */
public class ReflectionUtils {
  /**
   * Create an instance of the specified class with constuctor
   * matching the argument array.
   * @param clazz name of the class
   * @param args argument array
   * @param <T> type fo the class
   * @return instance of the class, or null if anything went wrong
   */
  public static <T> T createInstance(String clazz, Object... args) {
    Objects.requireNonNull(clazz, "null class name");
    try {
      Class classObj = Class.forName(clazz);
      Class[] argTypes = new Class[args.length];
      IntStream.range(0, args.length).forEach(i -> argTypes[i] = args[i].getClass());
      Constructor<T> ctor = classObj.getDeclaredConstructor(argTypes);
      return ctor.newInstance(args);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Write a private field with reflection.
   * @param object instance whose field is to be accessed
   * @param field name of private field
   * @param value value to be set to the field
   * @param <T> type of the field
   * @return the new value just set or null if failed
   */
  public static <T> T setField(Object object, String field, T value) {
    Objects.requireNonNull(object, "null target object");
    Objects.requireNonNull(field, "null field name");

    try {
      Field fieldObj = object.getClass().getDeclaredField(field);
      fieldObj.setAccessible(true);
      fieldObj.set(object, value);
      return value;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Read a private field with reflection.
   * @param object instance whose field is to be accessed
   * @param field name of private field
   * @param <T> type of the field
   * @return the value of the field or null if failed
   */
  public static <T> T getField(Object object, String field) {
    Objects.requireNonNull(object, "null target object");
    Objects.requireNonNull(field, "null field name");

    try {
      Field fieldObj = object.getClass().getDeclaredField(field);
      fieldObj.setAccessible(true);
      return (T)fieldObj.get(object);
    } catch (Exception e) {
      return null;
    }
  }

}
