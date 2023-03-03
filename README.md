############################ MQConnector #########################################
Connector for MQ-Series that feeds Snowflake using Snowpipe Streaming API
Can be deployed within an MQ-Series Docker/Container or within a server/vm.


      ############## To help setup Docker Container ##############
https://developer.ibm.com/tutorials/mq-connect-app-queue-manager-containers/

Setup Docker Commands:
1) docker pull icr.io/ibm-messaging/mq:latest
2) docker images
3) docker volume create qm1data
4) docker run --hostname=mymq --env LICENSE=accept --env MQ_QMGR_NAME=QM1 --volume qm1data:/mnt/mqm --publish 1414:1414 --publish 9443:9443 --detach --env MQ_APP_PASSWORD=<MYPASSWORD> icr.io/ibm-messaging/mq 
4) docker ps (to get container id/name)
5) docker exec -u 0 -it <container_id> bash


Once inside container, it is pretty minimal.  Suggested installations:

1) Install a newer Java JDK:
cd /opt
curl https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz >openJDK11.tar.gz
gunzip openJDK11.tar.gz
tar -xvf openJDK11.tar
rm openJDK11.tar
cd jdk-11.0.2
bin/java -version
(add to your path)

2) Install Nano for a text editor:
curl http://mirror.centos.org/centos/7/os/x86_64/Packages/nano-2.3.1-10.el7.x86_64.rpm >nano.rpm
rpm -i nano.rpm
 
Can now install this Connector within this Docker/Container

      ############## To Setup this Connector ##############
Download latest Snowpipe Streaming API and get JAR (snowflake-ingest-sdk.jar - follow instructions):  https://github.com/snowflakedb/snowflake-ingest-java
docker cp /tmp/snowflake-ingest-sdk.jar <CONTAINER_ID>:/opt/MQConnector/lib/snowflake-ingest-sdk.jar

