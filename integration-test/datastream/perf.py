import os

LOGS_DIRECTORY = "/tmp/datastream/logs"
OUTPUT_DIRECTORY = "/tmp/datastream/results"


def machine_logs():
  return {
    # Comment this out for now to avoid a logging fetching error at the end
    # "Storage-Node" : [os.path.join(STORAGE_NODE_LOGS_DIR, "storage-node.log")],
    # "Kafka"        : [os.path.join(KAFKA_TOPIC_LOGS_DIR, "kafka-topic.log")],
    # "Helix"        : [os.path.join(HELIX_NODE_LOGS_DIR, "helix.log")],
  }

def naarad_logs():
  return {
    # Comment this out for now to avoid a logging fetching error at the end
    # 'AdditionServer': [],
  }


def naarad_config(config, test_name=None):
  return {
    # Comment this out for now to avoid a logging fetching error at the end
    # os.path.join(os.path.dirname(os.path.abspath(__file__)), "naarad.cfg")
    # 'AdditionServer': [],
  }

