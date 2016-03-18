package com.linkedin.datastream.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.stream.IntStream;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to simplify usage of Java reflection.
 */
public class ReflectionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ReflectionUtils.class);

  /**
   * Create an instance of the specified class with constuctor
   * matching the argument array.
   * @param clazz name of the class
   * @param args argument array
   * @param <T> type fo the class
   * @return instance of the class, or null if anything went wrong
   */
  @SuppressWarnings("unchecked")
  public static <T> T createInstance(String clazz, Object... args) {
    Validate.notNull(clazz, "null class name");
    try {
      Class<T> classObj = (Class<T>) Class.forName(clazz);
      Class<?>[] argTypes = new Class<?>[args.length];
      IntStream.range(0, args.length).forEach(i -> argTypes[i] = args[i].getClass());
      Constructor<T> ctor = classObj.getDeclaredConstructor(argTypes);
      return ctor.newInstance(args);
    } catch (Exception e) {
      LOG.warn("Failed to create instance for: " + clazz, e);
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
  public static <T> T setField(Object object, String field, T value) throws Exception {
    Validate.notNull(object, "null target object");
    Validate.notNull(field, "null field name");

    try {
      Field fieldObj = object.getClass().getDeclaredField(field);
      fieldObj.setAccessible(true);
      fieldObj.set(object, value);
      return value;
    } catch (Exception e) {
      LOG.warn(String.format("Failed to set field, object = %s field = %s value = %s", object, field, value), e);
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
  @SuppressWarnings("unchecked")
  public static <T> T getField(Object object, String field) throws Exception {
    Validate.notNull(object, "null target object");
    Validate.notNull(field, "null field name");

    try {
      Field fieldObj = object.getClass().getDeclaredField(field);
      fieldObj.setAccessible(true);
      return (T) fieldObj.get(object);
    } catch (Exception e) {
      LOG.warn(String.format("Failed to get field, object = %s field = %s", object, field), e);
      return null;
    }
  }

  private static final Class<?>[][] COMPATIBLE_TYPES = {
    { Character.TYPE, Character.class },
    { Byte.TYPE, Byte.class },
    { Boolean.TYPE, Boolean.class },
    { Short.TYPE, Short.class },
    { Integer.TYPE, Integer.class },
    { Long.TYPE, Long.class },
    { Float.TYPE, Float.class },
    { Double.TYPE, Double.class },
  };

  private static <S, T> boolean isCompatiblePrimitive(Class<S> type1, Class<T> type2) {
    for (int i = 0; i < COMPATIBLE_TYPES.length; i++) {
      if ((type1.equals(COMPATIBLE_TYPES[i][0]) && type2.equals(COMPATIBLE_TYPES[i][1])) ||
          (type1.equals(COMPATIBLE_TYPES[i][1]) && type2.equals(COMPATIBLE_TYPES[i][0]))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Find method with more intelligence to support below two scenarios:
   *
   * 1) argument type is subtype of the corresponding parameter type.
   * 2) either argument or paramter type is primitive and the other is the boxed type
   *
   * @param clazz class which the method belongs
   * @param methodName name of the method
   * @param argTypes array of argument types
   * @return the method
   */
  public static Method findMatchingMethod(Class<?> clazz, String methodName, Class<?>... argTypes) {
    Method[] methods = clazz.getDeclaredMethods();
    Method foundMethod = null;
    for (Method method : methods) {
      if (method.getName().equals(methodName)) {
        Class<?>[] paramTypes = method.getParameterTypes();
        if (paramTypes.length == argTypes.length) {
          boolean matched = true;
          for (int i = 0; i < paramTypes.length; i++) {
            if (!paramTypes[i].isAssignableFrom(argTypes[i]) &&
                !isCompatiblePrimitive(paramTypes[i], argTypes[i])) {
              matched = false;
              break;
            }
          }

          if (matched) {
            foundMethod = method;
            break;
          }
        }
      }
    }
    return foundMethod;
  }

  /**
   * Call a method with its name regardless of accessibility.
   *
   * @param object target object to whom a method is to be invoked
   * @param methodName name of the method
   * @param args arguments for the method
   * @param <T> return type
   * @return return value of the method, null for void methods
   */
  @SuppressWarnings("unchecked")
  public static <T> T callMethod(Object object, String methodName, Object... args) throws Exception {
    Validate.notNull(object, "null class name");
    Method method = null;
    boolean isAccessible = true;
    try {
      Class<?>[] argTypes = new Class<?>[args.length];
      IntStream.range(0, args.length).forEach(i -> argTypes[i] = args[i].getClass());
      method = findMatchingMethod(object.getClass(), methodName, argTypes);
      if (method == null) {
        throw new NoSuchMethodException(methodName);
      }
      isAccessible = method.isAccessible();
      method.setAccessible(true);
      return (T) method.invoke(object, args);
    } catch (Exception e) {
      LOG.warn("Failed to invoke method: " + methodName, e);
      throw e;
    } finally {
      if (method != null) {
        method.setAccessible(isAccessible);
      }
    }
  }
}
