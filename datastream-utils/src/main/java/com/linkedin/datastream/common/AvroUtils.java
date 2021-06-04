/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;


/**
 * Utility methods for encoding/decoding data into/from Avro.
 */
public class AvroUtils {

  /**
   * Encode an Avro record into byte array
   *
   * @param clazz The class type of the Avro record
   * @param record the instance of the Avro record
   * @param <T> The type of the Avro record.
   * @return encoded bytes
   */
  public static <T> byte[] encodeAvroSpecificRecord(Class<T> clazz, T record) throws IOException {
    DatumWriter<T> msgDatumWriter = new SpecificDatumWriter<>(clazz);
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
    msgDatumWriter.write(record, encoder);
    encoder.flush();
    return os.toByteArray();
  }

  /**
   * Encode an Avro record into byte array
   * @param schema schema describing the desired layout of the bytes
   * @param record the instance of the Avro record
   * @return encoded bytes
   */
  public static byte[] encodeAvroIndexedRecord(Schema schema, IndexedRecord record) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    return encodeAvroIndexedRecord(schema, record, outputStream, encoder);
  }

  /**
   * Convert an Avro record to Json and encode it into byte array
   * @param schema schema describing the desired layout of the bytes
   * @param record the instance of the Avro record
   * @return encoded bytes
   */
  public static byte[] encodeAvroIndexedRecordAsJson(Schema schema, IndexedRecord record) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
    return encodeAvroIndexedRecord(schema, record, outputStream, encoder);
  }

  private static byte[] encodeAvroIndexedRecord(Schema schema, IndexedRecord record,
      ByteArrayOutputStream outputStream, Encoder encoder) throws IOException {
    DatumWriter<IndexedRecord> datumWriter;
    // IndexedRecord like SpecificRecordBase can extend both SpecificRecord and GenericRecord and
    // needs to be encoded using SpecificDatumWriter instead of GenericDatumWriter.
    if (record instanceof SpecificRecord) {
      datumWriter = new SpecificDatumWriter<>(schema);
    } else {
      datumWriter = new GenericDatumWriter<>(schema);
    }

    datumWriter.write(record, encoder);
    encoder.flush();
    outputStream.close();
    return outputStream.toByteArray();
  }

  /**
   * Decode and deserialize the byte array into an instance of an Avro record
   * @param clazz class of the instance to decode. It must be a valid Avro record.
   * @param bytes bytes to decode
   * @param <T> type of the instance to decode
   * @return decoded instance of T
   */
  public static <T extends SpecificRecord> T decodeAvroSpecificRecord(Class<T> clazz, byte[] bytes) throws IOException {
    return decodeAvroSpecificRecord(clazz, bytes, null);
  }

  /**
   * Decode and deserialize the byte array into an instance of an Avro record
   * @param clazz class of the instance to decode. It must be a valid Avro Record.
   * @param bytes bytes to decode
   * @param reuse existing instance of T that may be used to populate with the decoded result. There is no guarantee it
   *              will actually be used.
   * @param <T> type of the instance to decode
   * @return decoded instance of T
   */
  public static <T extends SpecificRecord> T decodeAvroSpecificRecord(Class<T> clazz, byte[] bytes, T reuse)
      throws IOException {
    SpecificDatumReader<T> reader = new SpecificDatumReader<>(clazz);
    Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    return reader.read(reuse, decoder);
  }

  /**
   * Decode and deserialize the byte array into an instance of an Avro record
   * @param schema schema describing the expected information of the bytes, valid for type T
   * @param bytes bytes to decode
   * @param reuse existing instance of T that may be used to populate with the decoded result. There is no guarantee it
   *              will actually be used.
   * @param <T> type of the instance to decode
   * @return decoded instance of T
   */
  public static <T extends SpecificRecord> T decodeAvroSpecificRecord(Schema schema, byte[] bytes, T reuse)
      throws IOException {
    BinaryDecoder binDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
    return reader.read(reuse, binDecoder);
  }

  /**
   * Decode and deserialize the byte array into an instance of an Avro record
   * @param schema schema describing the expected information of the bytes, valid for type T
   * @param bytes bytes to decode
   * @param reuse existing instance of T that may be used to populate with the decoded result. There is no guarantee it
   *              will actually be used.
   * @param <T> type of the instance to decode
   * @return decoded instance of T
   */
  public static <T> T decodeAvroGenericRecord(Schema schema, byte[] bytes, T reuse) throws IOException {
    BinaryDecoder binDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    GenericDatumReader<T> reader = new GenericDatumReader<>(schema);
    return reader.read(reuse, binDecoder);
  }

  /**
   * Decode and deserialize the byte array into an instance of an Avro record
   * @param schema schema describing the expected information of the bytes.
   * @param bytes bytes to decode
   * @return decoded instance of GenericRecord
   */
  public static GenericRecord decodeAvroGenericRecord(Schema schema, byte[] bytes) throws IOException {
    return decodeAvroGenericRecord(schema, bytes, null);
  }

  /**
   * Decode and deserialize the Json byte array into an instance of an Avro record
   * @param schema schema describing the expected information of the bytes.
   * @param bytes Json string in bytes to decode
   * @return decoded instance of GenericRecord
   */
  public static <T> T decodeJsonAsAvroGenericRecord(Schema schema, byte[] bytes, T reuse) throws IOException {
    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, new String(bytes, StandardCharsets.UTF_8));
    GenericDatumReader<T> reader = new GenericDatumReader<>(schema);
    return reader.read(reuse, jsonDecoder);
  }
}
