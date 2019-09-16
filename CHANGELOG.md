# Change Log
All notable changes to this project will be documented in this file.  
This project adheres to [Semantic Versioning](http://semver.org/).

## 1.0.1 — 2019-09-16

- Remove FindBugs showProgress property
- Remove reference for DatastreamProducerRecord from the sendcallback (#645)
- Make coordinator wait for event process thread to complete (#644)
- Default partition strategy always sends data to single partition (#637)
- Fix a bug in moving partition when there is a paused instance (#643)
- Publish auto-generated pegasus/Rest.li jars
- Enable FindBugs and fix detected issues
- Upgrade metrics-core to version 4.10 (#641)
- Kafka partition movement (#631)
- Fix datastream task prefix equality check in GroupIdConstructor
- Remove topic list from KMM event metadata (#638)
- Bug fix with custom SerDe being overriden by kafka factory implementations
- Refactor KMMConnector interface (#634)
- Fix a bug when putting an event with metadata in event queue (#632)
- Fix max_partition metric (#628)
- Fix a race condition in handleChangeAssignment (#626)
- Avoid creating a KafkaPositionTracker with a consumer that has a group id (#627)
- Fix the metric for max partition count in the task (#625)
- Increase poll timeout for TestCoordinator (#624)
- Kafka partition management (#618)
- Move project version to maven.gradle
- Replace flaky Bintray badge
- Correct Brooklin download location
- Follow redirects when downloading Brooklin
- Include version shield on README
- Add change log

## 1.0.0 — 2019-07-14
Initial open-source release

### Connectors
  - [KafkaConnector](https://github.com/linkedin/brooklin/wiki/Kafka-Connector)
  - [KafkaMirrorMakerConnector](https://github.com/linkedin/brooklin/wiki/Kafka-MirrorMaker-Connector)
  
### Transport Providers
  - [KafkaTransportProvider](https://github.com/linkedin/brooklin/wiki/Kafka-Transport-Provider)
  
