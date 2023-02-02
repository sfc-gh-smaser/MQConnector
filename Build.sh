#!/bin/sh
javac -cp "lib/snowflake-ingest-sdk-1.0.2-beta.7.jar:lib/slf4j-api-2.0.6.jar:lib/slf4j-simple-2.0.6.jar:classes:/opt/mqm/java/lib/*" -d classes src/snowflake/mq/*.java
jar cfm MQConnector.jar manifest.txt -C classes snowflake
