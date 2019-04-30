# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure("2") do |config|
  # The most common configuration options are documented and commented below.
  # For a complete reference, please see the online documentation at
  # https://docs.vagrantup.com.

  # Every Vagrant development environment requires a box. You can search for
  # boxes at https://vagrantcloud.com/search.
  config.vm.box = "centos/7"

  # Disable automatic box update checking. If you disable this, then
  # boxes will only be checked for updates when the user runs
  # `vagrant box outdated`. This is not recommended.
  # config.vm.box_check_update = false

  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine. In the example below,
  # accessing "localhost:8080" will access port 80 on the guest machine.
  # NOTE: This will enable public access to the opened port
  # config.vm.network "forwarded_port", guest: 80, host: 8080

  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine and only allow access
  # via 127.0.0.1 to disable public access
  # config.vm.network "forwarded_port", guest: 80, host: 8080, host_ip: "127.0.0.1"

  # Create a private network, which allows host-only access to the machine
  # using a specific IP.
  # config.vm.network "private_network", ip: "192.168.33.10"

  # Create a public network, which generally matched to bridged network.
  # Bridged networks make the machine appear as another physical device on
  # your network.
  # config.vm.network "public_network"

  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  # config.vm.synced_folder "../data", "/vagrant_data"

  # Provider-specific configuration so you can fine-tune various
  # backing providers for Vagrant. These expose provider-specific options.
  # Example for VirtualBox:
  #
  # config.vm.provider "virtualbox" do |vb|
  #   # Display the VirtualBox GUI when booting the machine
  #   vb.gui = true
  #
  #   # Customize the amount of memory on the VM:
  #   vb.memory = "1024"
  # end
  #
  # View the documentation for the provider you are using for more
  # information on available options.

  # Enable provisioning with a shell script. Additional provisioners such as
  # Puppet, Chef, Ansible, Salt, and Docker are also available. Please see the
  # documentation for more information about their specific syntax and use.

  config.vm.provision "shell", inline: <<-SHELL
    function exitIfError {
      RESULT=$?
      if [ $RESULT -ne 0 ]; then
        >&2 echo "$1"
        exit 1
      fi
    }

    # Install Java
    echo "Installing Java"
    yum -y update
    yum install -y java-1.8.0-openjdk
    exitIfError "Encountered an error while installing Java"

    # Download and untar Kafka
    echo "Downloading Kafka"
    curl -s -o kafka_2.12-2.2.0.tgz 'https://www-us.apache.org/dist/kafka/2.2.0/kafka_2.12-2.2.0.tgz'
    exitIfError "Encountered an error while downloading Kafka"

    tar -xzf kafka_2.12-2.2.0.tgz
    exitIfError "Encountered an error while uncompressing Kafka tarball"
    cd kafka_2.12-2.2.0

    # Start ZooKeeper
    echo "Starting ZooKeeper"
    bin/zookeeper-server-start.sh config/zookeeper.properties >/dev/null 2>&1 &
    exitIfError "Encountered an error while starting ZooKeeper server"

    kafka_server_ids=( src dest )
    kafka_server_ports=( 9093 9092 )

    for ((i=0;i<${#kafka_server_ids[@]};++i)); do
      suffix=${kafka_server_ids[i]}
      port=${kafka_server_ports[i]}

      # Create copy of config for Kafka server
      cp config/server.properties config/server-$suffix.properties

      # Edit config to give the 2 servers different root dirs and ports
      sed -ie "s~/tmp/kafka-logs~/tmp/kafka-logs/$suffix~; s~localhost:2181~localhost:2181/$suffix~" config/server-$suffix.properties
      printf "\nlisteners=PLAINTEXT://:$port" >> config/server-$suffix.properties

      # Start Kafka server
      echo "Starting $suffix Kafka server"
      bin/kafka-server-start.sh config/server-$suffix.properties >/dev/null 2>&1 &
      exitIfError "Encountered an error while starting $suffix Kafka server"
    done
    
    kafka_topic_prefixes=( first second third )
    for prefix in "${kafka_topic_prefixes[@]}"; do

      topic_name=$prefix-topic

      # Create topics in source Kafka server
      echo "Creating Kafka topic $topic_name in src Kafka server"
      bin/kafka-topics.sh --topic $topic_name --bootstrap-server localhost:${kafka_server_ports[0]} --create --replication-factor 1 --partitions 1 >/dev/null 2>&1
      exitIfError "Encountered an error while creating $topic_name"

      # Publish LICENSE to first-topic, NOTICE to second-topic and third-topic
      if [ $prefix = "first" ]; then
        filename="LICENSE"
      else
        filename="NOTICE"        
      fi

      echo "Publishing data to $topic_name in src Kafka server"
      cat $filename | bin/kafka-console-producer.sh --topic $topic_name --broker-list localhost:${kafka_server_ports[0]} >/dev/null 2>&1
      exitIfError "Encountered an error while publishing data to $topic_name"
    done

    # Download and untar Brooklin
    echo "Downloading Brooklin"
    cd ..
    cp /vagrant/brooklin-1.0.0-SNAPSHOT.tgz .
    exitIfError "Encountered an error while downloading Brooklin"

    tar -xzf brooklin-1.0.0-SNAPSHOT.tgz
    exitIfError "Encountered an error while uncompressing Brooklin tarball"

    # Start Brooklin server
    echo "Starting Brooklin"
    cd brooklin-1.0.0-SNAPSHOT
    bin/brooklin-server-start.sh config/server.properties > /dev/null 2>&1 &
    exitIfError "Encountered an error while starting Brooklin"

    # Suppress Open JDK's multi-processor warning
    echo '#!/usr/bin/env bash
      export KAFKA_OPTS="-XX:-AssumeMP"
      export OPTS="-XX:-AssumeMP"' > /etc/profile.d/kafka-brooklin-vars.sh
  SHELL
end
