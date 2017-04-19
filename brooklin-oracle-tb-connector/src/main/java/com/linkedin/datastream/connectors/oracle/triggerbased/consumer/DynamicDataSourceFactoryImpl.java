package com.linkedin.datastream.connectors.oracle.triggerbased.consumer;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DynamicDataSourceFactoryImpl implements OracleDataSourceFactory {

  private static final Logger LOG = LoggerFactory.getLogger(OracleDataSourceFactory.class);

  private static final Map<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();  // we should have 4 entries at most

  /**
   * dynamically create the OracleDataSource used to get DB connection(s).
   *
   * @param uri  URI of the desired data source
   * @param queryTimeoutSec  timeout for setting the connection properties of the data source; -1 = infinite
   * @return DataSource object for the specified URI
   */
  public DataSource createOracleDataSource(String uri, int queryTimeoutSec) throws Exception {
    DataSource ds = null;
    try {
      Class<?> oracleDataSourceClass = loadClass();
      Object ods = oracleDataSourceClass.newInstance();
      ds = (DataSource) ods;

      // TODO?  These reflection calls are almost certainly pigs as well, though not nearly as bad as
      // (the old version of) loadClass().
      Method setURLMethod = oracleDataSourceClass.getMethod("setURL", String.class);
      Method getConnectionPropertiesMethod = oracleDataSourceClass.getMethod("getConnectionProperties");
      Method setConnectionPropertiesMethod =
          oracleDataSourceClass.getMethod("setConnectionProperties", Properties.class);
      setURLMethod.invoke(ods, uri);

      Properties prop = (Properties) getConnectionPropertiesMethod.invoke(ods);
      if (prop == null) {
        prop = new Properties();
      }

      if (queryTimeoutSec > 0) {
        // read timeout should be slightly more than query timeout
        int readTimeoutMS = 1000 * queryTimeoutSec + 100;
        prop.put("oracle.jdbc.ReadTimeout", Integer.toString(readTimeoutMS));
      }
      setConnectionPropertiesMethod.invoke(ods, prop);
    } catch (Exception ex) {
      String errMsg = "Error trying to create an Oracle DataSource";
      LOG.error(errMsg, ex);
      throw ex;
    }
    return ds;
  }

  /**
   * @return  the dynamically loaded class
   */
  private static Class<?> loadClass() throws ClassNotFoundException {
    String className = "oracle.jdbc.pool.OracleDataSource";

    if (CLASS_CACHE.containsKey(className)) {
      return CLASS_CACHE.get(className);
    }

    Class<?> loadedClass = null;

    try {
      loadedClass = OracleDataSourceFactory.class.getClassLoader().loadClass(className);
      CLASS_CACHE.put(className, loadedClass);
    } catch (ClassNotFoundException ex) {
      LOG.error("Error loading a class " + className + " from ojdbc jar", ex);
      throw ex;
    }

    return loadedClass;
  }

}
