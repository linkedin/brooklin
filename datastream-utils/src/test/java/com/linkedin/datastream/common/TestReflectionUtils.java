/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;
import org.testng.annotations.Test;


public class TestReflectionUtils {
  private static final String STR_ARG1 = "hello";
  private static final Integer INT_ARG2 = 100;

  public TestReflectionUtils() {
  }

  public TestReflectionUtils(String first) {
    Assert.assertEquals(first, STR_ARG1);
  }

  public TestReflectionUtils(String first, Integer second) {
    Assert.assertEquals(first, STR_ARG1);
    Assert.assertEquals(second, INT_ARG2);
  }

  @Test
  public void testCreateInstance()
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException,
             InvocationTargetException {
    TestReflectionUtils utils;
    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName());
    Assert.assertNotNull(utils);

    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName(), STR_ARG1);
    Assert.assertNotNull(utils);

    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName(), STR_ARG1, INT_ARG2);
    Assert.assertNotNull(utils);

    utils = ReflectionUtils.createInstance(TestReflectionUtils.class.getCanonicalName(), STR_ARG1, STR_ARG1);
    Assert.assertNull(utils);

    utils = ReflectionUtils.createInstance("Foobar");
    Assert.assertNull(utils);

    utils = ReflectionUtils.createInstance("Foobar", 200);
    Assert.assertNull(utils);

    boolean exception = false;
    try {
      ReflectionUtils.createInstance((String) null);
    } catch (Exception e) {
      exception = true;
    }
    Assert.assertTrue(exception);
  }

  static class TestData {
    private String _privateField;
    public String _publicField;
  }

  @Test
  public void testSetField() throws Exception {
    TestData data = new TestData();
    Assert.assertEquals(ReflectionUtils.setField(data, "_privateField", "world"), "world");
    Assert.assertEquals("world", ReflectionUtils.getField(data, "_privateField"));

    Assert.assertEquals(ReflectionUtils.setField(data, "_publicField", "hello"), "hello");
    Assert.assertEquals("hello", ReflectionUtils.getField(data, "_publicField"));
  }

  private void privateVoidMethod(float foo, String bar) {
    System.out.println("privateVoidMethod: " + String.valueOf(foo) + " " + bar);
  }

  private int privateIntMethod(Integer foo) {
    System.out.println("privateIntMethod: " + String.valueOf(foo));
    return foo + 5;
  }

  public int publicNoArgsMethod() {
    System.out.println("publicPlainMethod");
    return 10;
  }

  private int privateSetArgMethod(Set<Integer> mySet) {
    System.out.println("privateSetArgMethod");
    return 10;
  }

  class A {
  }

  class B extends A {
  }

  private void privateSubtypeUnhappy(B b) {
  }

  private void privateSubtypeHappy(A a) {
  }

  @Test
  public void testCallMethod() throws Exception {
    TestReflectionUtils tester = new TestReflectionUtils();
    ReflectionUtils.callMethod(tester, "privateVoidMethod", 10.5f, "Hello");
    int retVal = ReflectionUtils.callMethod(tester, "privateIntMethod", 10);
    Assert.assertEquals(retVal, 15);

    // private method should stay private
    Method method = getClass().getDeclaredMethod("privateIntMethod", Integer.class);
    Assert.assertEquals(method.isAccessible(), false);

    retVal = ReflectionUtils.callMethod(tester, "publicNoArgsMethod");
    Assert.assertEquals(retVal, 10);

    Set<Integer> dummySet = new HashSet<>();
    retVal = ReflectionUtils.callMethod(tester, "privateSetArgMethod", dummySet);
    Assert.assertEquals(retVal, 10);

    try {
      ReflectionUtils.callMethod(tester, "privateSubtypeUnhappy", new A());
      Assert.fail();
    } catch (NoSuchMethodException e) {
    }

    ReflectionUtils.callMethod(tester, "privateSubtypeHappy", new B());
  }
}
