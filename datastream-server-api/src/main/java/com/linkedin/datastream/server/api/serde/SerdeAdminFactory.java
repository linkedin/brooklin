/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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
