# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## 5.1.0 - 2023-01-19
* Version 5.1.0-SNAPSHOT by @ehoner in https://github.com/linkedin/brooklin/pull/923
* Add support for post datastream create,update,delete and state change action by @hshukla in https://github.com/linkedin/brooklin/pull/915
* Version 4.2.0-SNAPSHOT by @ryannedolan in https://github.com/linkedin/brooklin/pull/914
* Update zookeeper dependency to import from LinkedIn published version by @surajkn in https://github.com/linkedin/brooklin/pull/917
* Set datastream status to deleting for delete call by @hshukla in https://github.com/linkedin/brooklin/pull/918
* Bump version after release by @hshukla in https://github.com/linkedin/brooklin/pull/920
* BMM Restart Improvements Part 1. Leader Coordinator Issuing Assignment Tokens by @jzakaryan in https://github.com/linkedin/brooklin/pull/919

## New Contributors
* @hshukla made their first contribution in https://github.com/linkedin/brooklin/pull/915

**Full Changelog**: https://github.com/linkedin/brooklin/compare/4.1.0...5.1.0

## 4.1.0 — 2022-09-29

- Refactored StickyPartitionAssignmentStrategy and implemented task estimation logic in LoadBasedPartitionAssignmentStrategy #835
- Fix flaky test testConsumeFromMultipleTopics #838
- Refactor Optional parameters in the constructor of strategy #836
- Implemented logic to prevent tasks from having more than specified number of partitions #837
- Publishing artifacts to JFrog #839
- Make the Throughput based assignment and task estimation based on partition assignment configurable #841
- Implemented metrics for LoadBasedPartitionAssignmentStrategy #840
- Clear the CallbackStatus entry from the map in FlushlessEventProducerHandler #843
- Handle the new session after session expiry #770
- Fixed issue with config key #847
- Print count of orphaned tasks and orphaned task locks in the log message #844
- Datastream stop transition redesign #842
- Fix flaky stopping simultaneous datastream test #849
- Update the DatabaseChunkedReader to take the Connection as input rather than the DataSource #850
- Handle leaking TP exception in handleAssignmentChange #845
- Migrating from AdminUtils with AdminClient #848
- Fix running task data structure logic in AbstractKafkaConnector for failing to stop task #851
- Removing partial Helix ZkClient dependency #852
- Stop the tasks in parallel in AbstractKafkaConnector #853
- Add additional log in LoadBasedPartitionAssignmentStrategy #856
- Fixing flaky test testCreateDatastreamHappyPathDefaultRetention #854
- Added toString() override in PartitionThroughputInfo #858
- Add Stats to DatastreamTaskImpl #855
- Make PartitionThroughputProvider metricsAware #859
- Added base strategy metric info in LoadBasedPartitionAssignmentStrategy #857
- Add alert metrics to identify that elastic task configurations require adjustment #860
- Fix restartDeadTask logic when the task thread has died #861
- Fix metric infos in PartitionThroughputProvider #862
- Fix the metrics deregistration in AbstractKafkaConnector when multiple stop are called #865
- Fix logging in LoadBasedPartitionAssignmentStrategy #866
- Make Default byte in rate and Msg in rate configurable #864
- Metrics are getting emitted in LoadBasedPartitionAssignmentStrategy only when it needs adjustment #867
- Use topic level throughput information when partition level information is unavailable #871
- Fix compilation error #874
- Loadbased Partition assigner not using topic level metrics to recognize partitions #876
- Flushless producer supporting both comparable and non comparable offsets #873
- LiveInstanceProvider subscription should be done only by the leader coordinator #879
- Fixed issue with missing exception message during task initialization #882
- Kafka upgrade #881
- Skipping onPartitionsRevoked during consumer.close() call #886
- Scala 2.12 upgrade #895
- Upgrade avro and move jackson from codehaus to fasterxml #894
- Fix topic deletion when multiple duplicate streams expire at the same time #897
- Use 2.4.1.57 kafka version #901
- Tests for min/max partitions per task metrics and minor code quality improvements #887
- Fix rebalancing-tasks bug and added tests #900
- Refactor Stopping Tasks On Assignment Change of Tasks #868
- Change python dependency in commit-msg git hook #904
- Remove Scala Dependencies #905
- Introduce broadcast API to TransportProvider #903
- Dedupe tasks on LeaderDoAssignment #906
- Fix Stopping Logic and Maintain Stopping Latch Counter #877
- Fixing test OnAssignmentChangeMultipleReassignments #908
- Update kafka version #910
- Replace 101tec ZkClient with Helix ZkClient #909
- Add retry to query retention time for destination topic #863
- Upgrade Zookeeper version to 3.6.3 #913

## 1.0.2 — 2019-10-01

- Relax Kafka broker hostname validation checks (#656)
- Log affected datastream on serialization errors (#659)
- Fix an issue in retrying the partition assignment (#654)
- Various position tracker improvements (#636)
- Enforce JMX metrics name generation logic used in io.dropwizard.metrics prior to v4.1.0-rc2 (#658)
- Invoke topic manger when the partitions are assigned (#657)
- Add custom checkpoint datastream metadata field to override connector level custom checkpoint (#653)
- Replace SlidingTimeWindowReservoir with SlidingTimeWindowArrayReservoir and reduce Histogram time (#655)
- Fix the datastream state for multiple tasks and connector validation (#646)
- Bump up version

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

