/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.serde;

import java.util.Optional;

/**
 * Generate a {@link SerDe} set
 */
public class SerDeSet {

  private final Optional<SerDe> _envelopeSerDe;
  private final Optional<SerDe> _keySerDe;
  private final Optional<SerDe> _valueSerDe;

  /**
   * Construct an instance of SerDeSet using the given key, value, and envelope SerDes, all of which could be Null.
   */
  public SerDeSet(SerDe keySerDe, SerDe valueSerDe, SerDe envelopeSerDe) {
    _keySerDe = Optional.ofNullable(keySerDe);
    _valueSerDe = Optional.ofNullable(valueSerDe);
    _envelopeSerDe = Optional.ofNullable(envelopeSerDe);
  }

  public Optional<SerDe> getKeySerDe() {
    return _keySerDe;
  }

  public Optional<SerDe> getValueSerDe() {
    return _valueSerDe;
  }

  public Optional<SerDe> getEnvelopeSerDe() {
    return _envelopeSerDe;
  }
}
