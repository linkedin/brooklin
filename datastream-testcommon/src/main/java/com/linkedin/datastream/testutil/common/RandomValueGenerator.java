package com.linkedin.datastream.testutil.common;

import java.util.Random;

public class RandomValueGenerator
{

  private String validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"; //.,/';\\][=-`<>?\":|}{+_)(*&^%$#@!~";
  private Random rand;

  // raghu todo - need proper constructor set and also decide on logging seed value and/or caching seed value here and a method to return it??
  //            - do we need a constructor w/ Random obj passed??
  // to help reproducibility of failed tests
  public RandomValueGenerator(long seed) {
    rand = new Random(seed);
  }

  public RandomValueGenerator() {
    rand = new Random();
  }

  public int getNextInt()
  {
    return rand.nextInt();
  }


  // to make it inclusive of min and max for the range, add 1 to the difference
  public int getNextInt(int min, int max)
  {
    if( max == min ) return min;
    // assert(max > min);

    return (rand.nextInt(max - min + 1) + min);
  }

  public String getNextString(int min, int max)
  {
    int length = getNextInt(min, max);

    StringBuffer strbld = new StringBuffer();
    for( int i = 0; i < length; i++)
    {
      char ch = validChars.charAt(rand.nextInt(validChars.length()));
      strbld.append(ch);
    }

    return strbld.toString();
  }

  public double getNextDouble() {
    return rand.nextDouble();
  }

  public float getNextFloat() {
    return rand.nextFloat();
  }

  public long getNextLong() {
    long randomLong = rand.nextLong();

    return randomLong == Long.MIN_VALUE ? 0: Math.abs(randomLong);
  }

  public boolean getNextBoolean()
  {
    return rand.nextBoolean();
  }

  public  byte[] getNextBytes(int maxBytesLength)
  {
    byte[] bytes = new byte[this.getNextInt(0, maxBytesLength)];
    rand.nextBytes(bytes);
    return bytes;
  }
}
