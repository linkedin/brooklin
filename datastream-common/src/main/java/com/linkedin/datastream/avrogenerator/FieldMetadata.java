/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.avrogenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.jetbrains.annotations.NotNull;

import com.google.common.base.Preconditions;


/**
 * Encapsulates metadata about an Oracle column/field.
 */
public class FieldMetadata {

  private static final String META_LIST_DELIMITER = ";";
  private static final String META_VALUE_DELIMITER = "=";
  private static final String COL_NAME = "dbFieldName";
  private static final String COL_POSITION = "dbFieldPosition";

  private final String _dbFieldName;
  private final String _nullable;
  private final Optional<Integer> _numberPrecision;
  private final Optional<Integer> _numberScale;

  private int _dbFieldPosition;
  private Types _dbFieldType;

  public FieldMetadata(@NotNull String dbFieldName, String nullable, int dbFieldPosition, @NotNull Types dbFieldType,
      Optional<Integer> numberPrecision, Optional<Integer> numberScale) {
    _dbFieldName = dbFieldName;
    _nullable = nullable;
    _dbFieldPosition = dbFieldPosition;
    _dbFieldType = dbFieldType;
    _numberPrecision = numberPrecision;
    _numberScale = numberScale;
  }

  /**
   * Parses a "meta" String into a map of key-value pairs.
   *
   * Ex. input of "dbTableName=ANET_TOPICS;pk=anetId;" will return a map of:
   *     "dbTableName" -> "ANET_TOPICS"
   *     "pk" -> "anetId"
   *
   * @param meta the metadata String
   * @return map of key-value pairs
   */
  public static Map<String, String> parseMetadata(String meta) {
    // cut off the trailing delimiter ";"
    if (meta.endsWith(META_LIST_DELIMITER)) {
      meta = meta.substring(0, meta.length() - 1);
    }

    String[] parts = meta.split(META_LIST_DELIMITER);
    Map<String, String> metas = new HashMap<>(parts.length);
    for (String part : parts) {
      String[] keyValuePair = part.split(META_VALUE_DELIMITER);
      if (keyValuePair.length != 2) {
        throw new IllegalArgumentException("Ill-formatted meta key-value pair: " + part);
      }
      String key = keyValuePair[0];
      String value = keyValuePair[1];

      metas.put(key, value);
    }

    return metas;
  }

  public static FieldMetadata fromString(String meta) {
    Map<String, String> metas = parseMetadata(meta);

    // field name, field position, and field type are mandatory
    String fieldName = Preconditions.checkNotNull(metas.get(COL_NAME),
        String.format("Missing metadata %s from meta string %s", COL_NAME, meta));
    int fieldPosition = Integer.valueOf(Preconditions.checkNotNull(metas.get(COL_POSITION),
        String.format("Missing metadata %s from meta string %s", COL_POSITION, meta)));
    Types fieldType = Types.fromString(Preconditions.checkNotNull(metas.get(FieldType.FIELD_TYPE_NAME),
        String.format("Missing metadata %s from meta string %s", FieldType.FIELD_TYPE_NAME, meta)));
    String nullable = Optional.ofNullable(metas.get(FieldType.NULLABLE)).orElse("");

    Optional<Integer> numberPrecision =
        Optional.ofNullable(metas.get(FieldType.PRECISION)).map(s -> Integer.valueOf(s));
    Optional<Integer> numberScale =
        Optional.ofNullable(metas.get(FieldType.SCALE)).map(s -> Integer.valueOf(s));

    return new FieldMetadata(fieldName, nullable, fieldPosition, fieldType, numberPrecision, numberScale);
  }

  public String getDbFieldName() {
    return _dbFieldName;
  }

  public String getNullable() {
    return _nullable;
  }

  public int getDbFieldPosition() {
    return _dbFieldPosition;
  }

  public Types getDbFieldType() {
    return _dbFieldType;
  }

  public Optional<Integer> getNumberPrecision() {
    return _numberPrecision;
  }

  public Optional<Integer> getNumberScale() {
    return _numberScale;
  }
}
