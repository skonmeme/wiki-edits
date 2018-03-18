package com.skt.skon.wikiedits

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource
import com.skt.skon.wikiedits.aggregator._
import com.skt.skon.wikiedits.config._

object WikipediaAnalysis {

  def main(args: Array[String]) {
    val wikipediaConfig = WikipediaAnalysisConfig.get(args, "Wikipedia Edits analyzer")

    val kafkaProducerProperties = new Properties()
    kafkaProducerProperties.put("bootstrap.servers", wikipediaConfig.brokers.mkString(","))
    kafkaProducerProperties.put("max.request.size", wikipediaConfig.kafkaMaxRequestSize.toString)
    kafkaProducerProperties.put("transaction.timeout.ms", wikipediaConfig.kafkaTransactionMaxTimeout.toString)

    val wikiProducer = new FlinkKafkaProducer011[String](
      wikipediaConfig.topic,
      new SimpleStringSchema,
      kafkaProducerProperties
    )
    wikiProducer.setWriteTimestampToKafka(true)

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

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
      .keyBy(_.getUser)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .aggregate(new WikipediaEditEventAggregate)

    val toKafka = wikiEdits
      .setParallelism(1)
      .addSink(wikiProducer)

    val toConsole = wikiEdits
      .setParallelism(1)
      .print

    environment.execute
  }

}
