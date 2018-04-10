package com.skt.skon.wikiedits.dataSources

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}

object WikipediaDataSources  {
  def combine(environment: StreamExecutionEnvironment, channels: Seq[String]): DataStream[WikipediaEditEvent] = {
    var dataStreams: DataStream[WikipediaEditEvent] = null
    for (channel <- channels) {
      dataStreams = dataStreams match {
        case null => environment.addSource(new WikipediaEditsSource("irc.wikimedia.org", 6667, "#" + channel + ".wikipedia"))
        case _ => dataStreams.union(environment.addSource(new WikipediaEditsSource("irc.wikimedia.org", 6667, "#" + channel + ".wikipedia")))
      }
    }
    dataStreams
  }
}
