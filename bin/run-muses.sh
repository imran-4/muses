#!/bin/bash

echo "Checking prerequisites..."


echo "Running Muses..."
#example command
$HADOOP_HOME/bin/yarn jar muses-yarn-client/target/muses-yarn-client-1.0.0-SNAPSHOT.jar de.tuberlin.dima.bdapro.muses.yarnclient.Client -jar muses-yarn-appmaster/target/muses-yarn-appmaster-1.0.0-SNAPSHOT.jar -number_of_containers 2 -app_jar muses-starter/target/muses-starter-1.0.0-SNAPSHOT.jar -conf muses-config.json -akka_conf muses-akka/target/application.conf

exit 0