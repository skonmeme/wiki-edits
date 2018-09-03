#!/bin/sh

export HADOOP_CLASSPATH=`hadoop classpath`
export FLINK_CONF_DIR='/Volumes/Contents HD/SK/DACoE/skon/wiki-edits/conf'

jar=$(ls build/libs/wiki-edits*.jar | sort -V | tail -n 1)

ENTRYCLASS="com.skt.skon.wikiedits.WikipediaAnalysis"

PARAM="--channel-list en,ko,jp,de,es,fr,ru,pt,it,zh,pl \
--session-gap 60 \
--brokers 192.168.10.254:9092 \
--topic-summary wiki-edits-summary --topic-contents wiki-edits-contents \
--group-id other \
--checkpoint-data-uri hdfs://skonuniverse:8020/flink \
--checkpoint-state-backend fs \
--checkpoint-interval $((5*1000*60))"

flink run \
  --jobmanager yarn-cluster \
  --yarndetached \
  --yarncontainer 2 \
  --yarnqueue default \
  --yarnjobManagerMemory 1024m \
  --yarntaskManagerMemory 1024m \
  --yarnslots 2 \
  --class $ENTRYCLASS \
  $jar \
  $PARAM
