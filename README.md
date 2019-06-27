# Brooklin
[![License](https://img.shields.io/github/license/linkedin/brooklin.svg?style=popout)](https://github.com/linkedin/brooklin/blob/master/LICENSE)
[![Last Commit](https://img.shields.io/github/last-commit/linkedin/brooklin.svg?style=popout)](https://github.com/linkedin/brooklin/commits/master)
[![Bugs](https://img.shields.io/github/issues/linkedin/brooklin/bug.svg?color=orange?style=popout)](https://github.com/linkedin/brooklin/labels/bug)
[![Gitter](https://img.shields.io/gitter/room/linkedin/kafka.svg?style=popout)](https://gitter.im/linkedin/brooklin)

![Brooklin Overview](images/brooklin-overview.svg)

Brooklin is a distributed system intended for streaming data between various heterogeneous source and destination systems with high reliability and throughput at scale. Designed for multitenancy, Brooklin can simultaneously power hundreds of data pipelines across different systems and can easily be extended to support new sources and destinations.

## Distinguishing Features

#### Extensible for Any Source/Destination System
- Brooklin offers a flexible API that can be extended to support a wide variety of source and destination systems. It is not confined to single type of source or destination system.
- Source and destination systems can be freely mixed and matched. They do not have to be the same.

#### Scalable
- Brooklin supports creating an arbitrary number of data streams that are processed concurrently and independently such that errors in one stream are isolated from the rest.
- Brooklin supports partitioned data streams throughout its core implementation and APIs.
- Brooklin can be deployed to a cluster of machines (scale out) to support as many data streams as desired.

#### Easy to Operate and Manage
- Brooklin exposes a REST endpoint for managing data streams, that offers a rich set of operations on them in addition to CRUD (e.g. `pause` and `resume`).
- Brooklin also exposes a diagnostics REST endpoint that enables on-demand querying of a data stream's status.

#### Battle-tested at Scale with Kafka
While it is not limited to any particular system, Brooklin provides capabilities for reading/writing massive amounts of data to/from Kafka with high reliability at scale. You can learn more about this in the [Use Cases](#use-cases) section.

#### Supports Change Data Capture with Bootstrap
- Brooklin supports propagating [Change Data Capture](https://en.wikipedia.org/wiki/Change_data_capture) events from data stores, e.g. RDBMS, KV stores ... etc. 
- Brooklin also supports streaming a snapshot of the existing data before propagating change events.

## Use Cases

#### Mirroring Kafka Clusters
Brooklin offers various advantages when used for mirroring data across Kafka clusters:

##### Multitenancy
A single Brooklin cluster can be used to mirror data across several Kafka clusters.

##### Fault Isolation Across Topic Partitions
One bad partition will not affect an entire Kafka topic. Mirroring will continue for all the other healthy partitions.

##### Whitelisting Topics Using Regular Expressions
Select the topics to mirror using regular expression patterns against their names.

##### Pausing/Resuming Individual Partitions
Through its [Datastream Management Service (DMS)](https://github.com/linkedin/brooklin/wiki/Brooklin-Architecture#rest-endpoints), Brooklin exposes REST APIs that allow finer control over replication pipelines, like being able to pause and resume individual partitions of a Kafka topic.

> Check out [Mirroring Kafka Clusters](https://github.com/linkedin/brooklin/wiki/mirroring-kafka-clusters) to learn more about using Brooklin to mirror Kafka clusters.

#### Change Data Capture
- Brooklin supports propagating [Change Data Capture](https://en.wikipedia.org/wiki/Change_data_capture) events from data stores, e.g. RDBMS, KV stores ... etc. 
- Brooklin supports bootstrapping data from a datastore, i.e. streaming a snapshot of the existing data before any change events.
- MySQL support is currently under development.

#### Stream Processing Bridge
Brooklin can be used as the underlying streaming infrastructure feeding data to Stream Processing systems, e.g. [Apache Samza](http://samza.apache.org/), [Apache Storm](https://storm.apache.org/), [Apache Spark](https://spark.apache.org/), [Apache Flink](https://flink.apache.org/).


## Trying out Brooklin
Feel free to check out our [step-by-step tutorials](https://github.com/linkedin/brooklin/wiki/test-driving-brooklin) for running Brooklin locally in a few example scenarios.

## Documentation
[Brooklin Wiki Pages](https://github.com/linkedin/Brooklin/wiki)

## Community
- Join our [Brooklin chat room on Gitter](https://gitter.im/linkedin/brooklin)
- File a bug or request features using [GitHub issues](https://github.com/linkedin/Brooklin/issues)

## Contributing
- [Developer Guide](https://github.com/linkedin/Brooklin/wiki/Developer-Guide)
- [Code of Conduct](https://github.com/linkedin/brooklin/blob/master/CODE_OF_CONDUCT.md)

## License
Copyright (c) LinkedIn Corporation. All rights reserved.

Licensed under the [BSD 2-Clause](https://github.com/linkedin/brooklin/blob/master/LICENSE) License.
