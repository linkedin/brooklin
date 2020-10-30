/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.translator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.function.Function;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.OTHER;
import static java.sql.Types.REAL;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

/**
 *  Class to implement translation between ResultSet and internal type
 */
public class ResultSetTranslator implements RecordTranslator<ResultSet, ArrayList<GenericRecord>>, SchemaTranslator<ResultSet, Schema> {

    private static final String CLASS_NAME = ResultSetTranslator.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
    public static final int MAX_DIGITS_IN_BIGINT = 19;
    public static final int MAX_DIGITS_IN_INT = 9;

    public static final int DEFAULT_PRECISION_VALUE = 10;
    public static final int DEFAULT_SCALE_VALUE = 0;
    public static final String AVRO_SCHEMA_NAMESPACE = "com.wayfair.brooklin";
    public static final String AVRO_SCHEMA_RECORD_NAME = "sqlRecord";
    public static final String SOURCE_SQL_DATA_TYPE = "source.sql.datetype";


    @Override
    public ArrayList<GenericRecord> translateToInternalFormat(ResultSet rs) throws SQLException {
        Schema schema = this.translateSchemaToInternalFormat(rs);
        ArrayList<GenericRecord> outPutArray = new ArrayList<>();
        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        while (rs.next()) {
            GenericRecord rec = new GenericData.Record(schema);
            for (int i = 1; i <= nrOfColumns; i++) {
                final int javaSqlType = meta.getColumnType(i);
                final Schema fieldSchema = schema.getFields().get(i - 1).schema();

                if (javaSqlType == BLOB) {
                    rec.put(i - 1, null);
                    continue;
                }
                Object value;
                if (javaSqlType == -101
                        || javaSqlType == -102) {
                    try {
                        value = rs.getTimestamp(i);
                        // Some drivers (like Derby) return null for getTimestamp() but return a Timestamp object in getObject()
                        if (value == null) {
                            value = rs.getObject(i);
                        }
                    } catch (Exception e) {
                        // The cause of the exception is not known, but we'll fall back to call getObject() and handle any "real" exception there
                        value = rs.getObject(i);
                    }
                } else if (javaSqlType == TIMESTAMP // 93
                        || javaSqlType == TIMESTAMP_WITH_TIMEZONE // 2014
                        || javaSqlType == microsoft.sql.Types.SMALLDATETIME // -150 represents TSQL smalldatetime type
                        || javaSqlType == microsoft.sql.Types.DATETIME // -151 represents TSQL datetime type
                        || javaSqlType == microsoft.sql.Types.DATETIMEOFFSET) { // -155 represents TSQL Datetimeoffset type

                    try {
                        // Convert datetime, datetime2 and datetimeoffset to UTC then convert to timestamp millis
                        value = rs.getTimestamp(i).getTime();
                        // Some drivers (like Derby) return null for getTimestamp() but return a Timestamp object in getObject()
                        if (value == null) {
                            value = rs.getObject(i);
                        }
                    } catch (Exception e) {
                        // The cause of the exception is not known, but we'll fall back to call getObject() and handle any "real" exception there
                        value = rs.getObject(i);
                    }
                } else {
                    value = rs.getObject(i);
                }

                if (value == null) {
                    rec.put(i - 1, null);

                } else if (javaSqlType == BINARY || javaSqlType == VARBINARY || javaSqlType == LONGVARBINARY || javaSqlType == ARRAY) {
                    // bytes requires little bit different handling
                    byte[] bytes = rs.getBytes(i);
                    ByteBuffer bb = ByteBuffer.wrap(bytes);
                    rec.put(i - 1, bb);
                } else if (javaSqlType == 100) { // Handle Oracle BINARY_FLOAT data type
                    rec.put(i - 1, rs.getFloat(i));
                } else if (javaSqlType == 101) { // Handle Oracle BINARY_DOUBLE data type
                    rec.put(i - 1, rs.getDouble(i));
                } else if (value instanceof Byte) {
                    // tinyint(1) type is returned by JDBC driver as java.sql.Types.TINYINT
                    // But value is returned by JDBC as java.lang.Byte
                    // (at least H2 JDBC works this way)
                    // direct put to avro record results:
                    // org.apache.avro.AvroRuntimeException: Unknown datum type java.lang.Byte
                    rec.put(i - 1, ((Byte) value).intValue());
                } else if (value instanceof Short) {
                    //MS SQL returns TINYINT as a Java Short, which Avro doesn't understand.
                    rec.put(i - 1, ((Short) value).intValue());
                } else if (value instanceof BigDecimal) {
                    // Delegate mapping to AvroTypeUtil in order to utilize logical types.
                    rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, fieldSchema));

                } else if (value instanceof BigInteger) {
                    // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                    // It the SQL type is BIGINT and the precision is between 0 and 19 (inclusive); if so, the BigInteger is likely a
                    // long (and the schema says it will be), so try to get its value as a long.
                    // Otherwise, Avro can't handle BigInteger as a number - it will throw an AvroRuntimeException
                    // such as: "Unknown datum type: java.math.BigInteger: 38". In this case the schema is expecting a string.
                    if (javaSqlType == BIGINT) {
                        int precision = meta.getPrecision(i);
                        if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                            rec.put(i - 1, value.toString());
                        } else {
                            try {
                                rec.put(i - 1, ((BigInteger) value).longValueExact());
                            } catch (ArithmeticException ae) {
                                // Since the value won't fit in a long, convert it to a string
                                rec.put(i - 1, value.toString());
                            }
                        }
                    } else {
                        rec.put(i - 1, value.toString());
                    }

                } else if (value instanceof Number || value instanceof Boolean) {
                    if (javaSqlType == BIGINT) {
                        int precision = meta.getPrecision(i);
                        if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                            rec.put(i - 1, value.toString());
                        } else {
                            rec.put(i - 1, value);
                        }
                    } else if ((value instanceof Long) && meta.getPrecision(i) < MAX_DIGITS_IN_INT) {
                        int intValue = ((Long) value).intValue();
                        rec.put(i - 1, intValue);
                    } else {
                        rec.put(i - 1, value);
                    }

                } else if (value instanceof Date) {
                    // Delegate mapping to AvroTypeUtil in order to utilize logical types.
                    rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, fieldSchema));
                } else if (value instanceof java.sql.SQLXML) {
                    rec.put(i - 1, ((SQLXML) value).getString());
                } else {
                    // The different types that we support are numbers (int, long, double, float),
                    // as well as boolean values and Strings. Since Avro doesn't provide
                    // timestamp types, we want to convert those to Strings. So we will cast anything other
                    // than numbers or booleans to strings by using the toString() method.
                    rec.put(i - 1, value.toString());
                }
            }
            outPutArray.add(rec);
        }
        return outPutArray;
    }

    /**
     * Creates an Avro schema from a result set. If the table/record name is known a priori and provided, use that as a
     * fallback for the record name if it cannot be retrieved from the result set, and finally fall back to a default value.
     *
     * @param rs The result set to convert to Avro
     * @return A Schema object representing the result set converted to an Avro record
     * @throws SQLException if any error occurs during conversion
     */
    @Override
    public Schema translateSchemaToInternalFormat(ResultSet rs) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        String tableName = AVRO_SCHEMA_RECORD_NAME;
        if (nrOfColumns > 0) {
            String tableNameFromMeta = meta.getTableName(1);
            if (!StringUtils.isBlank(tableNameFromMeta)) {
                tableName = tableNameFromMeta;
            }
        }

        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace(AVRO_SCHEMA_NAMESPACE).fields();

        /**
         * Some missing Avro types - Decimal, Date types. May need some additional work.
         */
        for (int i = 1; i <= nrOfColumns; i++) {
            /**
             *   as per jdbc 4 specs, getColumnLabel will have the alias for the column, if not it will have the column name.
             *  so it may be a better option to check for columnlabel first and if in case it is null is someimplementation,
             *  check for alias. Postgres is the one that has the null column names for calculated fields.
             */
            String nameOrLabel = StringUtils.isNotEmpty(meta.getColumnLabel(i)) ? meta.getColumnLabel(i) : meta.getColumnName(i);
            String columnName = nameOrLabel;
            String sqlType = null;
            switch (meta.getColumnType(i)) {
                case CHAR:
                case LONGNVARCHAR:
                case LONGVARCHAR:
                case NCHAR:
                case NVARCHAR:
                case VARCHAR:
                case CLOB:
                case NCLOB:
                case OTHER:
                case Types.SQLXML:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BIT:
                case BOOLEAN:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
                    break;

                case INTEGER:
                    if (meta.isSigned(i) || (meta.getPrecision(i) > 0 && meta.getPrecision(i) < MAX_DIGITS_IN_INT)) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                case SMALLINT:
                case TINYINT:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    break;

                case BIGINT:
                    // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                    // If the precision > 19 (or is negative), use a string for the type, otherwise use a long. The object(s) will be converted
                    // to strings as necessary
                    int precision = meta.getPrecision(i);
                    if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                // java.sql.RowId is interface, is seems to be database
                // implementation specific, let's convert to String
                case ROWID:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case FLOAT:
                case REAL:
                case 100: //Oracle BINARY_FLOAT type
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().floatType().endUnion().noDefault();
                    break;

                case DOUBLE:
                case 101: //Oracle BINARY_DOUBLE type
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion().noDefault();
                    break;

                // Since Avro 1.8, LogicalType is supported.
                case DECIMAL:
                case NUMERIC:

                    final int decimalPrecision;
                    final int decimalScale;
                    if (meta.getPrecision(i) > 0) {
                        // When database returns a certain precision, we can rely on that.
                        decimalPrecision = meta.getPrecision(i);
                        //For the float data type Oracle return decimalScale < 0 which cause is not expected to org.apache.avro.LogicalTypes
                        //Hence falling back to default scale if decimalScale < 0
                        decimalScale = meta.getScale(i) > 0 ? meta.getScale(i) : DEFAULT_SCALE_VALUE;
                    } else {
                        // If not, use default precision.
                        decimalPrecision = DEFAULT_PRECISION_VALUE;
                        // Oracle returns precision=0, scale=-127 for variable scale value such as ROWNUM or function result.
                        // Specifying 'oracle.jdbc.J2EE13Compliant' SystemProperty makes it to return scale=0 instead.
                        // Queries for example, 'SELECT 1.23 as v from DUAL' can be problematic because it can't be mapped with decimal with scale=0.
                        // Default scale is used to preserve decimals in such case.
                        decimalScale = meta.getScale(i) > 0 ? meta.getScale(i) : DEFAULT_SCALE_VALUE;
                    }
                    final LogicalTypes.Decimal decimal = LogicalTypes.decimal(decimalPrecision, decimalScale);
                    addNullableField(builder, columnName,
                            u -> u.type(decimal.addToSchema(SchemaBuilder.builder().bytesType())));

                    break;

                case DATE:
                    addNullableField(builder, columnName,
                            u -> u.type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType())));
                    break;

                case TIME:
                    addNullableField(builder, columnName,
                            u -> u.type(LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType())));
                    break;

                case -101: // Oracle's TIMESTAMP WITH TIME ZONE
                case -102: // Oracle's TIMESTAMP WITH LOCAL TIME ZONE
                    addNullableField(builder, columnName,
                            u -> u.type(LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType())));
                    break;

                case TIMESTAMP:
                case TIMESTAMP_WITH_TIMEZONE:
                case microsoft.sql.Types.SMALLDATETIME: // -150 represents TSQL smalldatetime type
                case microsoft.sql.Types.DATETIME: // -151 represents TSQL datetime type
                case microsoft.sql.Types.DATETIMEOFFSET: //  -155 represents TSQL DATETIMEOFFSET type
                    try {
                        sqlType = meta.getColumnTypeName(i);
                    } catch (SQLException ex) {
                        LOG.warn("Column type name not found for value {}", meta.getColumnType(i));
                    }
                    final Schema timestampMilliType = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());
                    if (sqlType != null) {
                        timestampMilliType.addProp(SOURCE_SQL_DATA_TYPE, sqlType);
                    }
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().type(timestampMilliType).endUnion().nullDefault();
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case ARRAY:
                case BLOB:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().bytesType().endUnion().noDefault();
                    break;


                default:
                    throw new IllegalArgumentException("createSchema: Unknown SQL type " + meta.getColumnType(i) + " / " + meta.getColumnTypeName(i)
                            + " (table: " + tableName + ", column: " + columnName + ") cannot be converted to Avro type");
            }
        }

        return builder.endRecord();
    }

    private static void addNullableField(
            SchemaBuilder.FieldAssembler<Schema> builder,
            String columnName,
            Function<SchemaBuilder.BaseTypeBuilder<SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>>,
                    SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>> func
    ) {
        final SchemaBuilder.BaseTypeBuilder<SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>>
                and = builder.name(columnName).type().unionOf().nullType().and();
        func.apply(and).endUnion().noDefault();
    }
}
