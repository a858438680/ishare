#!/bin/bash

WORK_DIR=`pwd`

TPCH_JAR=$WORK_DIR/middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar
SQL_JAR=$WORK_DIR/sql/core/target/scala-2.11/spark-sql_2.11-2.4.0.jar
EXAMPLE_JAR=$WORK_DIR/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.0.jar
CAT_JAR=$WORK_DIR/sql/catalyst/target/scala-2.11/spark-catalyst_2.11-2.4.0.jar
AVRO_JAR=$WORK_DIR/external/avro/target/scala-2.11/spark-avro_2.11-2.4.0.jar
KAFKA_JAR=$WORK_DIR/external/kafka-0-10-sql/target/scala-2.11/spark-sql-kafka-0-10_2.11-2.4.0.jar
CORE_JAR=$WORK_DIR/core/target/scala-2.11/spark-core_2.11-2.4.0.jar


#REMOTE=lincoln.cs.uchicago.edu
REMOTE=southport.cs.uchicago.edu
REMOTE_SPARK=$REMOTE:/tank/hdfs/sqp/spark

#REMOTE=grace.cs.uchicago.edu
#REMOTE_SPARK=$REMOTE:/mnt/hdd-2T-1/totem/spark/

#scp $TPCH_JAR $SQL_JAR $EXAMPLE_JAR $REMOTE_SPARK/jars/
scp $TPCH_JAR $REMOTE_SPARK/jars/
#scp $CAT_JAR $REMOTE_SPARK/jars/
scp $SQL_JAR $REMOTE_SPARK/jars/
#scp $TPCH_JAR $REMOTE_SPARK/jars/
#scp $TPCH_JAR $SQL_JAR $REMOTE_SPARK/jars/
#scp $CAT_JAR $REMOTE_SPARK/jars/
#scp $CORE_JAR $REMOTE_SPARK/jars/

#scp -r metadata/* $REMOTE_SPARK/metadata/

#scp $TPCH_JAR $SQL_JAR $CAT_JAR $AVRO_JAR $KAFKA_JAR $CORE_JAR $REMOTE_SPARK/jars/

