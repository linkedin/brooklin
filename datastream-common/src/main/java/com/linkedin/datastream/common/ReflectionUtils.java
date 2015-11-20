package com.linkedin.datastream.common;


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
}
