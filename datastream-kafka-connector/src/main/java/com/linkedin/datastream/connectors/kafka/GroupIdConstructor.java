package com.linkedin.datastream.connectors.kafka;

import com.linkedin.datastream.common.Datastream;


public interface GroupIdConstructor {
  String constructGroupId(Datastream datastream);
}
