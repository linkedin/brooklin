scriptDir=`pwd`"/"`dirname $0`
mysqlDir=$scriptDir"/../mysql/"

source $scriptDir"/props.inc"

echo "mysqlDirectory : ${mysqlDir}"

echo "Starting the mysql master..."

# Start the master
docker run --name ds-mysql-master -p ${masterPort}:${mysqlLocalPort} -v ${mysqlDir}/cnf/common:/etc/mysql -v ${mysqlDir}/cnf/master:/etc/mysql/conf.d  -e MYSQL_ROOT_PASSWORD=${mysqlPwd} -d  ${mysqlImage}

# Find the ip address of the master
masterIpAddress=`docker inspect --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ds-mysql-master`

echo "Master's IpAddress ${masterIpAddress}"

echo "Starting the mysql slave..."

echo "docker run --name ds-mysql-slave -p ${slavePort}:${mysqlLocalPort} -v ${mysqlDir}/cnf/common:/etc/mysql -v ${mysqlDir}/cnf/slave:/etc/mysql/conf.d  -e MYSQL_ROOT_PASSWORD=${mysqlPwd} -d ${mysqlImage}"

# Start the slave
docker run --name ds-mysql-slave -p ${slavePort}:${mysqlLocalPort} -v ${mysqlDir}/cnf/common:/etc/mysql -v ${mysqlDir}/cnf/slave:/etc/mysql/conf.d  -e MYSQL_ROOT_PASSWORD=${mysqlPwd} -d ${mysqlImage}

# Find the slave ip address
slaveIpAddress=`docker inspect --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ds-mysql-slave`

echo "Slave's IpAddress ${slaveIpAddress}"

echo "slaveIpAddress=${slaveIpAddress}" > /tmp/dsdockersetup.inc
echo "masterIpAddress=${masterIpAddress}" >> /tmp/dsdockersetup.inc