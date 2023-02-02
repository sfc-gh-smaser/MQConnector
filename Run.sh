#!/bin/sh
java  -cp "MQConnector.jar:/opt/mqm/java/lib/*" -Djava.library.path=/opt/mqm/java/lib64 --add-opens=java.base/java.nio=ALL-UNNAMED snowflake.mq.MQConnector mqconnector.properties

