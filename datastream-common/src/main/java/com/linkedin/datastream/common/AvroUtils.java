package com.linkedin.datastream.common;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * Created by spunuru on 6/15/15.
 */
public class AvroUtils {

  /**
   * Encode an Avro record into byte array
   *
   * @param cls The class type of the Avro record
   * @param event the instance of the avro record
   * @param <T> The type of the avro record.
   * @return
   * @throws java.io.IOException
   */
  public static <T> byte[] encodeAvroSpecificRecord(Class<T> cls, T event) throws IOException {

    DatumWriter<T> msgDatumWriter = new SpecificDatumWriter<T>(cls);
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    Encoder encoder = new BinaryEncoder(os);
    msgDatumWriter.write(event, encoder);
    encoder.flush();
    return os.toByteArray();
  }

  /**
   * Decode and deserialize the byte array into an instance of an Avro record
   * @param cls
   * @param eventBytes
   * @param <T>
   * @return
   * @throws java.io.IOException
   */
  public static <T> T decodeAvroSpecificRecord(Class<T> cls, byte[] eventBytes) throws IOException {
    DatumReader<T> reader = new SpecificDatumReader<T>(cls);
    Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(eventBytes, null);
    return reader.read(null, decoder);
  }

  public static GenericRecord decodeAvroGenericRecord(byte[] valueBytes, Schema schema)
      throws IOException {
    BinaryDecoder binDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(valueBytes, null);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
    return reader.read(null, binDecoder);
  }

  public static byte[] encodeAvroGenericRecord(Schema schema, GenericRecord r) throws IOException {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder binaryEncoder = new BinaryEncoder(outputStream);
    datumWriter.write(r, binaryEncoder);
    binaryEncoder.flush();
    outputStream.close();
    return outputStream.toByteArray();
  }
}
