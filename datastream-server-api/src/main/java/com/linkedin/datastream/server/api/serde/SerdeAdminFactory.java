package com.linkedin.datastream.server.api.serde;

import java.util.Properties;


/**
 * Factory that is used to create the SerdeAdmins
 */
public interface SerdeAdminFactory {

  /**
   * Method that is used to create the SerdeAdmin. This method is called once for every unique serdeName using
   * the SerdeAdminFactory.
   * @param serdeName Name of the serde
   * @param serdeConfig  SerdeConfig that can be used to create the SerdeAdmin
   * @return SerdeAdmin instance.
   */
  SerdeAdmin createSerdeAdmin(String serdeName, Properties serdeConfig);
}
