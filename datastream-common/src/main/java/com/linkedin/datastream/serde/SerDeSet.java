package com.linkedin.datastream.serde;

import java.util.Optional;


public class SerDeSet {

  private final Optional<SerDe> _envelopeSerDe;
  private final Optional<SerDe> _keySerDe;
  private final Optional<SerDe> _valueSerDe;

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
