# Datastream
Datastream is a database change-capture framework built on top of Kafka. Datastream provides the ability to host DB- and function-specific "connectors" that can produce change-capture events into Kafka and to support clients that wish to consume the change-capture data stream(s). Possible connector types include Espresso change-capture, Oracle bootstrap, and so on.

## Documentation

Check out the Datastream documentation at <https://go/datastream>.

## Getting Started

### Forking and cloning the github Datastream repo

First log in to github and make your own Datastream fork.  Go to <https://github.com/linkedin/Datastream> and poke the "Fork" button at the upper right, click on your personal repo, and you should be ready to go within a few seconds.

Once you've done that, on your dev box change into the parent directory of where you want your Datastream clone to live.  In the examples in this file, the final location is assumed to be "/path/to/local/github/Datastream", so you would do the following:

```shell
cd /path/to/local/github
git clone https://github.com/<your_github_username>/Datastream.git
```

That will create the new Datastream subdirectory.  Note that the git name is case-insensitive, so you could equally well clone "datastream.git" and end up with a lowercase "datastream" subdirectory.  (That could get confused with one of the internal repos, however.)

<https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Working+with+datastream+code> has complete details, but to complete the process and switch to a new branch for subsequent development, do the following:

```shell
cd /path/to/local/github/Datastream
git remote add upstream https://github.com/linkedin/datastream.git
git checkout origin/master
git pull upstream master
git checkout -b <your_feature_branch> origin/master
```

### Building, testing, and publishing Datastream

After cloning the Datastream repository, run the following commands to build the code:

```shell
cd /path/to/local/github/Datastream
./gradlew clean assemble
```

Note that gradlew is a simple wrapper-script that lives at the top level of the Datastream repo; it handles Gradle versioning.  (The OSS Datastream project is not compatible with the 2.2.x version used within LinkedIn, so the script uses version 1.10, downloading it first if necessary.)

To run unit tests:

```shell
cd /path/to/local/github/Datastream
./gradlew test
```

You can publish the Datastream binaries into your local maven repository by running:

```shell
./gradlew publishToMavenLocal
```

The binaries will appear under $HOME/.m2/repository/com/github/datastream/.

### Developing using Idea

You can use IntelliJ for developing Datastream. First build the IntelliJ project files by running:

```shell
./gradlew idea
```

Once the IntelliJ project files (*.ipr) are created, you can open them using IntelliJ and start developing.

### Contributing and submitting patches

Contributions are accepted in the form of pull requests. Please see <https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Working+with+datastream+code> for an overview of how the various internal and external repos interact, and our preferred development methodology. For details on how to submit pull requests, see <https://help.github.com/articles/using-pull-requests/>.

Before you submit the pull request, ensure that the changes in your fork build and unit tests succeed with the latest changes from upstream. To sync the changes from the main repository into your fork, you can follow the instructions in <https://help.github.com/articles/syncing-a-fork/>. Then do the following:

```shell
cd /path/to/local/github/Datastream
./gradlew clean assemble idea test
```

