package com.linkedin.datastream.common;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Util class for logging related methods
 */
public class LogUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LogUtils.class.getName());

  private static void printNumberRange(StringBuilder stringBuilder, int start, int tail) {
    if (start == tail) {
      stringBuilder.append(start);
    } else {
      stringBuilder.append(start).append("-").append(tail);
    }
  }

  /**
   * Shortening the list of integers by merging consecutive numbers together. e.g.
   * [1, 2, 4, 5, 6] -> [1-2, 4-6]
   * @param list list of integers to generate logging String for
   * @return compacted String that merges consecutive numbers
   */
  public static String logNumberArrayInRange(List<Integer> list) {
    if (list == null || list.isEmpty()) {
      return "[]";
    }
    try {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("[");
      List<Integer> copiedList = new ArrayList<>(list);
      copiedList.sort(Integer::compareTo);
      int curStart = copiedList.get(0);
      int curTail = curStart;
      for (int i = 1; i < copiedList.size(); i++) {
        int num = copiedList.get(i);
        if (num <= curTail + 1) {
          curTail = num;
        } else {
          printNumberRange(stringBuilder, curStart, curTail);
          stringBuilder.append(", ");
          curStart = num;
          curTail = num;
        }
      }
      printNumberRange(stringBuilder, curStart, curTail);
      stringBuilder.append("]");
      return stringBuilder.toString();
    } catch (Exception e) {
      LOG.error("Failed to generate string for the int list in range", e);
      return list.toString();
    }
  }
}
