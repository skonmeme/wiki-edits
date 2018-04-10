package com.skt.skon.wikiedits

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import com.skt.skon.wikiedits.aggregator._
import com.skt.skon.wikiedits.config._
import com.skt.skon.wikiedits.dataSources.WikipediaDataSources
import com.skt.skon.wikiedits.eventTime.WikipediaTimestampsAndWatermarks
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

object WikipediaAnalysis {

  def main(args: Array[String]) {
    val wikipediaConfig = WikipediaAnalysisConfig.get(args, "Wikipedia Edits analyzer")

    val kafkaProducerProperties = new Properties()
    kafkaProducerProperties.put("bootstrap.servers", wikipediaConfig.brokers.mkString(","))
    kafkaProducerProperties.put("max.request.size", wikipediaConfig.kafkaMaxRequestSize.toString)
    kafkaProducerProperties.put("transaction.timeout.ms", wikipediaConfig.kafkaTransactionMaxTimeout.toString)

    val wikiProducerSummary = new FlinkKafkaProducer011[String](
      wikipediaConfig.topicSummary,
      new SimpleStringSchema,
      kafkaProducerProperties
    )
    wikiProducerSummary.setWriteTimestampToKafka(true)

    // Generate Flink environmental settings
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(2)

    wikipediaConfig.checkpointStateBackend match {
      case MemoryStateBackend() =>
        environment.setStateBackend(new org.apache.flink.runtime.state.memory.MemoryStateBackend)
      case FsStateBackend(uri) =>
        environment.setStateBackend(new org.apache.flink.runtime.state.filesystem.FsStateBackend(uri))
      case RocksDBStateBackend(uri) =>
        environment.setStateBackend(new org.apache.flink.contrib.streaming.state.RocksDBStateBackend(uri))
      case _ => ()
    }

    wikipediaConfig.checkpointStateBackend match {
      case NoStateBackend() => ()
      case _ => {
        environment.enableCheckpointing(wikipediaConfig.checkpointInterval)
        environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
        environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(wikipediaConfig.checkpointInterval / 2)
        environment.getCheckpointConfig.setCheckpointTimeout(wikipediaConfig.checkpointInterval * 10)
        environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      }
    }

    val wikiEdits = WikipediaDataSources.combine(environment, wikipediaConfig.wikpediaChannels)
      .assignTimestampsAndWatermarks(new WikipediaTimestampsAndWatermarks)
      .keyBy(_.getUser)

    val toKafkaSummary = wikiEdits
      .window(EventTimeSessionWindows.withGap(Time.minutes(wikipediaConfig.sessionGap)))
      .aggregate(new WikipediaEditEventSummaryAggregate)
      .addSink(wikiProducerSummary)
      .setParallelism(1)

    if (wikipediaConfig.topicContents != "") {
      val wikiProducerContents = new FlinkKafkaProducer011[String](
        wikipediaConfig.topicContents,
        new SimpleStringSchema,
        kafkaProducerProperties
      )
      wikiProducerContents.setWriteTimestampToKafka(true)

      val toKafkaContents = wikiEdits
        .window(GlobalWindows.create)
        .trigger(CountTrigger.of(1))
        .aggregate(new WikipediaEditEventContentsAggregate)
        .addSink(wikiProducerContents)
        .setParallelism(1)
    }

    environment.execute("Wikipedia Edit logs")
  }

}
