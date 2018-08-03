package com.linkedin.datastream.common.databases;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.xml.transform.Result;
import javax.xml.transform.Source;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.DatastreamRuntimeException;


public class TestOracleTypeInterpreter {
  //CHECKSTYLE:OFF
  public static final Schema GENERIC_SCHEMA = Schema.parse("{\"name\":\"DaftPunk_Songs\",\"doc\":\"Auto-generatedAvroschemaformusic.sy$daftPunk.GeneratedatFeb10,201205:41:48PMPST\",\"type\":\"record\",\"meta\":\"dbFieldName=music.sy$daft_punk_songs;pk=getLucky;\",\"namespace\":\"com.linkedin.events.anet\",\"fields\":[{\"name\":\"txn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldType=NUMBER;dbFieldPosition=0;\"},{\"name\":\"getLucky\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=GET_LUCKY;dbFieldType=NUMBER;dbFieldPosition=1;\"},{\"name\":\"settings\",\"type\":[{\"name\":\"SETTINGS_T\",\"type\":\"record\",\"fields\":[{\"name\":\"settings\",\"type\":{\"items\":{\"name\":\"settingT\",\"type\":\"record\",\"meta\":\"dbFieldName=SETTINGS;dbFieldType=STRUCT;dbFieldPosition=0;\",\"fields\":[{\"name\":\"instantCrush\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=INSTANT_CRUSH;dbFieldType=NUMBER;dbFieldPosition=0;\"},{\"name\":\"oneMoreTime\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=ONE_MORE_TIME;dbFieldType=NUMBER;dbFieldPosition=1;\"},{\"name\":\"somethingAboutUs\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=SOMETHING_ABOUT_US;dbFieldType=VARCHAR;dbFieldPosition=2;\"},{\"name\":\"daFunk\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=DA_FUNK;dbFieldType=NUMBER;dbFieldPosition=3;\"},{\"name\":\"robotRock\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=ROBOT_ROCK;dbFieldType=NUMBER;dbFieldPosition=4;\"}]},\"name\":\"settingsArray\",\"type\":\"array\"}}]},\"null\"],\"meta\":\"dbFieldName=SETTINGS;dbFieldType=STRUCT;dbFieldPosition=17;\"},{\"name\":\"contact\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=CONTACT;dbFieldType=VARCHAR;dbFieldPosition=18;\"},{\"name\":\"digitalLove\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=DIGITAL_LOVE;dbFieldType=TIMESTAMP;dbFieldPosition=19;\"},{\"name\":\"humanAfterAll\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=HUMAN_AFTER_ALL;dbFieldType=VARCHAR;dbFieldPosition=20;\"},{\"name\":\"technologic\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=TECHNOLOGIC;dbFieldType= XMLTYPE;dbFieldPosition=20;\"}]}");
  public static final Schema COMPLEX_SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"BIG_CITIES_V12\",\"namespace\":\"com.linkedin.events.cities\",\"fields\":[{\"name\":\"key\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldType=NUMBER;dbFieldPosition=1;\"},{\"name\":\"tokyo\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=TOKYO;dbFieldType=CHAR;dbFieldPosition=32;\"},{\"name\":\"delhi\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=DELHI;dbFieldType=CHAR;dbFieldPosition=33;\"},{\"name\":\"geo\",\"type\":{\"type\":\"record\",\"name\":\"DATABUS_GEO_T\",\"fields\":[{\"name\":\"country\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=COUNTRY;dbFieldType=VARCHAR;dbFieldType=CHAR;dbFieldPosition=0;\"},{\"name\":\"geoPlaceMaskCode\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=GEO_PLACE_MASK_CODE;dbFieldType=CHAR;dbFieldPosition=5;\"},{\"name\":\"latitudeDeg\",\"type\":[\"float\",\"null\"],\"meta\":\"dbFieldName=LATITUDE_DEG;dbFieldType=NUMBER;dbFieldPosition=6;\"},{\"name\":\"longitudeDeg\",\"type\":[\"float\",\"null\"],\"meta\":\"dbFieldName=LONGITUDE_DEG;dbFieldType=NUMBER;dbFieldPosition=7;\"}]},\"meta\":\"dbFieldName=GEO;dbFieldPosition=34;dbFieldType=DATABUS_GEO_T;\"},{\"name\":\"mexicoCity\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=MEXICO_CITY;dbFieldType=CHAR;dbFieldPosition=41;\"},{\"name\":\"smallerCities\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"smallCityT\",\"fields\":[{\"name\":\"cityId\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=CITY_ID;dbFieldType=NUMBER;dbFieldPosition=0;\"},{\"name\":\"cityName\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=CITY_NAME;dbFieldType=VARCHAR2;dbFieldPosition=1;\"}],\"meta\":\"dbFieldName=SMALLER_CITIES;dbFieldType=ARRAY;dbFieldPosition=42;\"}}},{\"name\":\"osaka\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=OSAKA;dbFieldType=CHAR;dbFieldPosition=43;\"},{\"name\":\"newYork\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=NEW_YORK;dbFieldType=NUMBER;dbFieldPosition=74;\"}],\"meta\":\"dbFieldName=sy$BIG_CITIES;\"}");
  //CHECKSTYLE:ON


  @Test
  public void testGetChildSchemaValidUnion() {
    List<Type> acceptable = new ArrayList<>();
    acceptable.add(Type.LONG);

    // expect to not throw error
    Schema childSchema = OracleTypeInterpreter.getChildSchema(GENERIC_SCHEMA, "getLucky", acceptable);
  }

  @Test
  public void testGetChildSchemaValid() {
    Schema schema =
        org.apache.avro.Schema.parse("{\"name\":\"Feedback\",\"type\":\"record\",\"fields\":[{\"name\":\"lowLights\",\"type\":\"long\"}]}");

    List<Type> acceptable = new ArrayList<>();
    acceptable.add(Type.LONG);

    // expect to not throw error
    Schema childSchema = OracleTypeInterpreter.getChildSchema(schema, "lowLights", acceptable);
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testGetChildSchemaUnacceptableType() {
    Schema schema =
        org.apache.avro.Schema.parse("{\"name\":\"Feedback\",\"type\":\"record\",\"fields\":[{\"name\":\"lowLights\",\"type\":\"long\"}]}");

    List<Type> acceptable = new ArrayList<>();
    acceptable.add(Type.STRING);

    // expect to throw error
    Schema childSchema = OracleTypeInterpreter.getChildSchema(schema, "lowLights", acceptable);
  }

  @Test
  public void testSqlObjectToAvro() throws SQLException {
    String dbValue = "value";
    Object object = new OracleTypeInterpreter().sqlObjectToAvro(dbValue, "contact", GENERIC_SCHEMA);

    Assert.assertEquals(object, dbValue);
    Assert.assertTrue(object instanceof String);
  }

  @Test
  public void testSqlObjectToAvroNumeric() throws SQLException {
    BigDecimal dbValue = new BigDecimal(123L);

    // even though the argument is BigDecimal, we should convert to an int because
    // its defined as an int in the schema
    Object object = new OracleTypeInterpreter().sqlObjectToAvro(dbValue, "getLucky", GENERIC_SCHEMA);
    Assert.assertEquals(object, dbValue.longValue());
    Assert.assertTrue(object instanceof Long);
  }

  @Test
  public void testSqlObjectToAvroTimestamp() throws SQLException {
    Timestamp ts = new Timestamp(123L);

    Object object = new OracleTypeInterpreter().sqlObjectToAvro(ts, "digitalLove", GENERIC_SCHEMA);
    Assert.assertEquals(object, ts.getTime());
    Assert.assertTrue(object instanceof Long);
  }

  @Test
  public void testSqlObjectToAvroXML() throws SQLException {
    SQLXML ts = new SQLXML() {
      @Override
      public void free() throws SQLException {
      }
      @Override
      public InputStream getBinaryStream() throws SQLException {
        return null;
      }
      @Override
      public OutputStream setBinaryStream() throws SQLException {
        return null;
      }
      @Override
      public Reader getCharacterStream() throws SQLException {
        return null;
      }
      @Override
      public Writer setCharacterStream() throws SQLException {
        return null;
      }
      @Override
      public String getString() throws SQLException {
        return " <?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
            + " <!DOCTYPE Test SQLXML Type\n"
            + " <subTag:configuration random config\">\n"
            + "</subTag:configuration>";
      }
      @Override
      public void setString(String value) throws SQLException {
      }
      @Override
      public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        return null;
      }
      @Override
      public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        return null;
      }
    };

    Object object = new OracleTypeInterpreter().sqlObjectToAvro(ts, "technologic", GENERIC_SCHEMA);
    Assert.assertTrue(object instanceof String);
    Assert.assertEquals(object, ts.getString());
  }


  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testSqlObjectToAvroUnexpectedType() throws SQLException {
    Duration duration = Duration.ofMillis(10L);

    // we dont expect Duration types from ResultSets
    Object object = new OracleTypeInterpreter().sqlObjectToAvro(duration, "getLucky", GENERIC_SCHEMA);
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testNullUnion() {
    // ensure we handle the situation where the only types in a schema  are only one item
    Schema schema =
        Schema.parse("{\"name\":\"Feedback\",\"type\":\"record\",\"fields\":[{\"name\":\"lowLights\",\"type\": [\"null\"]}]}");

    List<Type> list = new ArrayList<>();
    list.add(Type.STRING);

    Schema child = OracleTypeInterpreter.getChildSchema(schema, "lowLights", list);
  }

  public void testStructGeneration() throws Exception {
    Object[] objects = new Object[4];
    String country = "a";
    String regionCode = "b";
    BigDecimal latitude  = new BigDecimal(11.0F);
    BigDecimal longitude = new BigDecimal(12.0F);

    objects[0] = country;
    objects[1] = regionCode;
    objects[2] = latitude;
    objects[3] = longitude;

    Struct struct = Mockito.mock(Struct.class);
    Mockito.when(struct.getAttributes()).thenReturn(objects);
    Schema structSchema = COMPLEX_SCHEMA.getField("geo").schema();

    GenericData.Record record =
        (GenericData.Record) (new OracleTypeInterpreter().sqlObjectToAvro(struct, "lowLights", structSchema));

    Assert.assertEquals(record.get("country"), country);
    Assert.assertEquals(record.get("geoPlaceMaskCode"), regionCode);
    Assert.assertEquals(record.get("latitudeDeg"), latitude.floatValue());
    Assert.assertEquals(record.get("longitudeDeg"), longitude.floatValue());
  }
}
