# Datastream
Datastream is a framework for change data capture system built on top of kafka. Datastream provides capability to write connectors that can produce change data capture events into Kafka.

## Documentation

Check out the datastream documentation at <http://go/datastream>

## Getting Started

### Building

To Build datastream, you need to build kafka first.

#### Building and publishing kafka 

Please follow the instructions at <https://github.com/apache/kafka/blob/trunk/README.md> to clone and build kafka on your local machine. Once kafka is built, you can publish the kafka binaires to local maven repository by running 

```shell
./gradlew install 
```

Once the kafka binaries are published. 

#### Building and publishing Datastream

Clone the datastream repository into /path/to/Datastream/localrepo and run the following commands

```shell
cd /path/to/Datastream/localrepo
./gradlew clean build
```

You can release the datastream binaries into local maven repository by running 

```shell
./gradlew publishToMavenLocal
```

### Contributing and submitting patches

Contributions are accepted in the form of pull requests, please use this <https://help.github.com/articles/using-pull-requests/> on how to submit the pull request. 

Before you submit the pull request, ensure that your changes in your fork builds and tests run with the latest changes from upstream. To sync the changes from the main repository into your fork you can follow the instructions here <https://help.github.com/articles/syncing-a-fork/>

