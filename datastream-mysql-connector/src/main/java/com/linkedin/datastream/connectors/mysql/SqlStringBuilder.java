package com.linkedin.datastream.connectors.mysql;

import java.util.Arrays;


public class SqlStringBuilder implements CharSequence {
  // SQL Statements are very rarely as little as 16 characters which is the default size used by
  // the default constructor for StringBuilder.
  private static final int INITIAL_CAPACITY = 128;

  private final StringBuilder _sqlString;

  public SqlStringBuilder() {
    this(INITIAL_CAPACITY);
  }

  public SqlStringBuilder(int capacity) {
    _sqlString = new StringBuilder(capacity);
  }

  public SqlStringBuilder(String str) {
    _sqlString = new StringBuilder(str);
  }

  public SqlStringBuilder(CharSequence seq) {
    _sqlString = new StringBuilder(seq);
  }

  /**
   * Appends a string to the Sql Statement
   *
   * @param str   the String to append
   * @return a reference to this object
   */
  public SqlStringBuilder append(String str) {
    _sqlString.append(str);
    return this;
  }

  /**
   * Appends a concatenated string with backticks (`) around it.
   * Note: The backtick is appended before and after the concatenated String, not individual Strings.
   * Backticks within each string are properly escaped.
   *
   * Example:
   * appendIdent("A","B") will return SqlStringBuilder representation of `AB`.
   * appendIdent("A","B`") will return SqlStringBuilder representation of `AB```.
   *
   * @param   strs  Strings to append
   * @return a reference to this object
   */
  public SqlStringBuilder appendIdent(CharSequence... strs) {
    _sqlString.append('`');
    for (CharSequence s : strs) {
      for (int i = 0; i < s.length(); i++) {
        char ch = s.charAt(i);
        if (ch == '`') {
          _sqlString.append('`');
        }
        _sqlString.append(ch);
      }
    }
    _sqlString.append('`');
    return this;
  }

  /**
   * Comma separates one or more CharSequence identifiers. Each identifier is appropriately wrapped around a backtick.
   *
   * Example:
   * appendCommandSeperatedIdent("A","B") will return the SqlStringBuilder representation of `A`,`B`
   * @param strs  One or more identifiers to return
   * @return A reference to this object
   */
  public <T extends CharSequence> SqlStringBuilder appendCommaSeperatedIdent(Iterable<T> strs) {
    if (strs == null) {
      return this;
    }

    int size = length();
    for (T str : strs) {
      appendIdent(str);
      append(',');
    }
    if (length() > size) {
      deleteCharAt(length() - 1); //remove the trailing comma
    }

    return this;
  }

  /**
   * Comma separates one or more CharSequence identifiers. Each identifier is appropriately wrapped around a backtick.
   *
   * Example:
   * appendCommandSeperatedIdent("A","B") will return the SqlStringBuilder representation of `A`,`B`
   * @param strs  One or more identifiers to return
   * @return A reference to this object
   */
  public SqlStringBuilder appendCommaSeperatedIdent(CharSequence... strs) {
    return appendCommaSeperatedIdent(Arrays.asList(strs));
  }

  /**
   * Appends a concatenated string with single quote (') around it.
   * Note: The single quote is appended before and after the concatenated String, not individual Strings.
   * Each String is properly escaped and concatenated.
   *
   * Example:
   * appendLiteral("A","B","C") will return SqlStringBuilder representation of "ABC".
   * appendLiteral("A","B'","C") will return SqlStringBuilder representation of "AB\'C".
   *
   * @param   strs  Strings to append
   * @return a reference to this object
   */
  public SqlStringBuilder appendLiteral(CharSequence... strs) {
    _sqlString.append("'");
    escapeAndAppendText(strs);
    _sqlString.append("'");
    return this;
  }

  /**
   * Comma-seperates one or more CharSequence literals. Each literal is appropriately escaped and wrapped around a single-quote
   *
   * Example:
   * appendCommaSeperatedLiterals("A","B") returns the SqlStringBuilder representation of 'A','B'
   *
   * @param strs  One or more CharSequence literal
   * @return a reference to this object
   */
  public SqlStringBuilder appendCommaSeperatedLiterals(CharSequence... strs) {
    return appendCommaSeperatedLiterals(Arrays.asList(strs));
  }

  /**
   * Comma-seperates one or more CharSequence literals. Each literal is appropriately escaped and wrapped around a single-quote
   *
   * Example:
   * appendCommaSeperatedLiterals("A","B") returns the SqlStringBuilder representation of 'A','B'
   *
   * @param strs  One or more CharSequence literal
   * @return a reference to this object
   */
  public <T extends CharSequence> SqlStringBuilder appendCommaSeperatedLiterals(Iterable<T> strs) {
    if (strs == null) {
      return this;
    }

    int size = length();
    for (T str : strs) {
      appendLiteral(str);
      append(',');
    }

    if (length() > size) {
      deleteCharAt(length() - 1); //remove the trailing comma
    }
    return this;
  }

  /**
   * @see java.lang.StringBuilder#length()
   */
  @Override
  public int length() {
    return _sqlString.length();
  }

  /**
   * @see java.lang.StringBuilder#capacity()
   */
  public int capacity() {
    return _sqlString.capacity();
  }

  /**
   * @see java.lang.StringBuilder#ensureCapacity(int)
   */
  public void ensureCapacity(int minimumCapacity) {
    _sqlString.ensureCapacity(minimumCapacity);
  }

  /**
   * @see java.lang.StringBuilder#trimToSize()
   */
  public void trimToSize() {
    _sqlString.trimToSize();
  }

  /**
   * @see java.lang.StringBuilder#setLength(int)
   */
  public void setLength(int newLength) {
    _sqlString.setLength(newLength);
  }

  /**
   * @see java.lang.StringBuilder#append(Object)
   */
  public SqlStringBuilder append(Object obj) {
    _sqlString.append(obj);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#append(StringBuffer)
   */
  public SqlStringBuilder append(StringBuffer sb) {
    _sqlString.append(sb);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#charAt(int)
   */
  @Override
  public char charAt(int index) {
    return _sqlString.charAt(index);
  }

  /**
   * @see java.lang.StringBuilder#append(CharSequence)
   */
  public SqlStringBuilder append(CharSequence s) {
    _sqlString.append(s);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#codePointAt(int)
   */
  public int codePointAt(int index) {
    return _sqlString.codePointAt(index);
  }

  /**
   * @see java.lang.StringBuilder#append(CharSequence, int, int)
   */
  public SqlStringBuilder append(CharSequence s, int start, int end) {
    _sqlString.append(s, start, end);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#append(char[])
   */
  public SqlStringBuilder append(char[] str) {
    _sqlString.append(str);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#append(char[], int, int)
   */
  public SqlStringBuilder append(char[] str, int offset, int len) {
    _sqlString.append(str, offset, len);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#append(boolean)
   */
  public SqlStringBuilder append(boolean b) {
    _sqlString.append(b);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#append(char)
   */
  public SqlStringBuilder append(char c) {
    _sqlString.append(c);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#append(int)
   */
  public SqlStringBuilder append(int i) {
    _sqlString.append(i);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#codePointBefore(int)
   */
  public int codePointBefore(int index) {
    return _sqlString.codePointBefore(index);
  }

  /**
   * @see java.lang.StringBuilder#append(long)
   */
  public SqlStringBuilder append(long lng) {
    _sqlString.append(lng);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#append(float)
   */
  public SqlStringBuilder append(float f) {
    _sqlString.append(f);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#append(double)
   */
  public SqlStringBuilder append(double d) {
    _sqlString.append(d);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#appendCodePoint(int)
   */
  public SqlStringBuilder appendCodePoint(int codePoint) {
    _sqlString.appendCodePoint(codePoint);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#delete(int, int)
   */
  public SqlStringBuilder delete(int start, int end) {
    _sqlString.delete(start, end);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#deleteCharAt(int)
   */
  public SqlStringBuilder deleteCharAt(int index) {
    _sqlString.deleteCharAt(index);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#replace(int, int, String)
   */
  public SqlStringBuilder replace(int start, int end, String str) {
    _sqlString.replace(start, end, str);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#codePointCount(int, int)
   */
  public int codePointCount(int beginIndex, int endIndex) {
    return _sqlString.codePointCount(beginIndex, endIndex);
  }

  /**
   * @see java.lang.StringBuilder#insert(int, char[], int, int)
   */
  public SqlStringBuilder insert(int index, char[] str, int offset, int len) {
    _sqlString.insert(index, str, offset, len);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#insert(int, Object)
   */
  public SqlStringBuilder insert(int offset, Object obj) {
    _sqlString.insert(offset, obj);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#insert(int, String)
   */
  public SqlStringBuilder insert(int offset, String str) {
    _sqlString.insert(offset, str);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#insert(int, char[])
   */
  public SqlStringBuilder insert(int offset, char[] str) {
    _sqlString.insert(offset, str);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#insert(int, CharSequence)
   */
  public SqlStringBuilder insert(int dstOffset, CharSequence s) {
    _sqlString.insert(dstOffset, s);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#offsetByCodePoints(int, int)
   */
  public int offsetByCodePoints(int index, int codePointOffset) {
    return _sqlString.offsetByCodePoints(index, codePointOffset);
  }

  /**
   * @see java.lang.StringBuilder#insert(int, CharSequence, int, int)
   */
  public SqlStringBuilder insert(int dstOffset, CharSequence s, int start, int end) {
    _sqlString.insert(dstOffset, s, start, end);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#insert(int, boolean)
   */
  public SqlStringBuilder insert(int offset, boolean b) {
    _sqlString.insert(offset, b);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#insert(int, char)
   */
  public SqlStringBuilder insert(int offset, char c) {
    _sqlString.insert(offset, c);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#insert(int, int)
   */
  public SqlStringBuilder insert(int offset, int i) {
    _sqlString.insert(offset, i);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#getChars(int, int, char[], int)
   */
  public void getChars(int srcBegin, int srcEnd, char[] dst, int dstBegin) {
    _sqlString.getChars(srcBegin, srcEnd, dst, dstBegin);
  }

  /**
   * @see java.lang.StringBuilder#insert(int, long)
   */
  public SqlStringBuilder insert(int offset, long l) {
    _sqlString.insert(offset, l);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#insert(int, float)
   */
  public SqlStringBuilder insert(int offset, float f) {
    _sqlString.insert(offset, f);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#insert(int, double)
   */
  public SqlStringBuilder insert(int offset, double d) {
    _sqlString.insert(offset, d);
    return this;
  }

  /**
   * @see java.lang.StringBuilder#indexOf(String)
   */
  public int indexOf(String str) {
    return _sqlString.indexOf(str);
  }

  /**
   * @see java.lang.StringBuilder#indexOf(String, int)
   */
  public int indexOf(String str, int fromIndex) {
    return _sqlString.indexOf(str, fromIndex);
  }

  /**
   * @see java.lang.StringBuilder#lastIndexOf(String)
   */
  public int lastIndexOf(String str) {
    return _sqlString.lastIndexOf(str);
  }

  /**
   * @see java.lang.StringBuilder#lastIndexOf(String, int)
   */
  public int lastIndexOf(String str, int fromIndex) {
    return _sqlString.lastIndexOf(str, fromIndex);
  }

  /**
   * @see java.lang.StringBuilder#reverse()
   */
  public SqlStringBuilder reverse() {
    _sqlString.reverse();
    return this;
  }

  /**
   * @see java.lang.StringBuilder#setCharAt(int, char)
   */
  public void setCharAt(int index, char ch) {
    _sqlString.setCharAt(index, ch);
  }

  /**
   * @see java.lang.StringBuilder#substring(int)
   */
  public String substring(int start) {
    return _sqlString.substring(start);
  }

  /**
   * @see java.lang.StringBuilder#subSequence(int, int)
   */
  @Override
  public CharSequence subSequence(int start, int end) {
    return _sqlString.subSequence(start, end);
  }

  /**
   * @see java.lang.StringBuilder#substring(int, int)
   */
  public String substring(int start, int end) {
    return _sqlString.substring(start, end);
  }

  private void escapeAndAppendText(CharSequence... strs) {
    for (CharSequence str : strs) {
      for (int i = 0; i < str.length(); i++) {
        char ch = str.charAt(i);
        switch (ch) {
          case 0:
            _sqlString.append("\\0");
            break;
          case '\'':
            _sqlString.append("\\'");
            break;
          case '"':
            _sqlString.append("\\\"");
            break;
          case '\b':
            _sqlString.append("\\b");
            break;
          case '\n':
            _sqlString.append("\\n");
            break;
          case '\r':
            _sqlString.append("\\r");
            break;
          case '\t':
            _sqlString.append("\\t");
            break;
          case 26:
            _sqlString.append("\\Z");
            break;
          case '\\':
            _sqlString.append("\\\\");
            break;
          default:
            _sqlString.append(ch);
            break;
        }
      }
    }
  }

  /**
   * @see java.lang.StringBuilder#toString()
   */
  @Override
  public String toString() {
    return _sqlString.toString();
  }

  /**
   * @see java.lang.StringBuilder#equals(Object)
   */
  @Override
  public boolean equals(Object obj) {
    return (this == obj) || (obj instanceof SqlStringBuilder && toString().equals(obj.toString()));
  }

  /**
   * @see java.lang.StringBuilder#hashCode()
   */
  @Override
  public int hashCode() {
    return _sqlString.hashCode();
  }
}
