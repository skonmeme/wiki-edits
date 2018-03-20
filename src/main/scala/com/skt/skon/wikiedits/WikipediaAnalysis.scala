package com.skt.skon.wikiedits

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource

import com.skt.skon.wikiedits.aggregator._
import com.skt.skon.wikiedits.config._
import com.skt.skon.wikiedits.eventTime.WikipediaTimestampsAndWatermarks

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
      case _ => environment.enableCheckpointing(wikipediaConfig.checkpointInterval)
    }

    val wikiEdits = environment
      .addSource(new WikipediaEditsSource("irc.wikimedia.org", 6667, "#en.wikipedia"))
      .assignTimestampsAndWatermarks(new WikipediaTimestampsAndWatermarks)
      .keyBy(_.getUser)
      .window(EventTimeSessionWindows.withGap(Time.minutes(1)))

    val toKafkaSummary = wikiEdits
      .aggregate(new WikipediaEditEventSummaryAggregate)
      .addSink(wikiProducerSummary)

    if (wikipediaConfig.topicContents != "") {
      val wikiProducerContents = new FlinkKafkaProducer011[String](
        wikipediaConfig.topicContents,
        new SimpleStringSchema,
        kafkaProducerProperties
      )
      wikiProducerContents.setWriteTimestampToKafka(true)

      val toKafkaContents = wikiEdits
        .aggregate(new WikipediaEditEventContentsAggregate)
        .addSink(wikiProducerContents)
    }

    val toConsole = wikiEdits
      .aggregate(new WikipediaEditEventSummaryAggregate)
      .print

    environment.execute
  }

}
