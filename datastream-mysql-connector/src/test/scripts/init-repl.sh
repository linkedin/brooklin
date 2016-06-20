
scriptDir=`pwd`"/"`dirname $0`
source $scriptDir"/props.inc"
source /tmp/dsdockersetup.inc

 # echo $localhost $masterPort $mysqlPwd
binlogFileName=`mysql -h ${localhost} -P ${masterPort} -u root -p${mysqlPwd} -e 'SHOW MASTER STATUS\G' | grep File | cut -d ':' -f 2 | tr -d ' '`
binlogPosition=`mysql -h ${localhost} -P ${masterPort} -u root -p${mysqlPwd} -e 'SHOW MASTER STATUS\G' | grep Position | cut -d ':' -f 2 | tr -d ' '`

echo "Hooking up slave to master at position - ${binlogFileName}:${binlogPosition}"

# Setup master and start replication
changeMasterCmd="CHANGE MASTER TO MASTER_HOST='${masterIpAddress}', MASTER_USER='root', MASTER_PASSWORD='${mysqlPwd}', MASTER_LOG_FILE='${binlogFileName}', MASTER_LOG_POS=${binlogPosition}; START SLAVE;"
echo "Executing ${changeMasterCmd} at ${slavePort}"
mysql -h ${localhost} -P ${slavePort} -u root -p${mysqlPwd} -e "${changeMasterCmd}" 
mysql -h ${localhost} -P ${slavePort} -u root -p${mysqlPwd} -e "SHOW SLAVE STATUS\G" 
