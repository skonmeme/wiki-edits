package com.skt.skon.wikiedits.aggregator

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent
import org.json4s._
import org.json4s.native.Serialization._

case class WikipediaEditSummary(user: String,
                                count: Long,
                                bytes: Long)

case class WikipediaEditContents(user: String,
                                 count: Long,
                                 urls: Array[String],
                                 summaries: Array[String])

class WikipediaEditEventSummaryAggregate extends AggregateFunction[WikipediaEditEvent, (String, Long, Long), String] {
  override def createAccumulator(): (String, Long, Long) =
    ("", 0, 0L)

  override def add(value: WikipediaEditEvent, accumulator: (String, Long, Long)) =
    (value.getUser, 1, value.getByteDiff + accumulator._2)

  override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) =
    (a._1, a._2 + b._2, a._3 + b._3)

  override def getResult(accumulator: (String, Long, Long)): String = {
    //write(WikipediaEditSummary(accumulator._1, accumulator._2, accumulator._3)))
    implicit val Formats = DefaultFormats
    write(accumulator)
  }
}

class WikipediaEditEventContentsAggregate extends AggregateFunction[WikipediaEditEvent, (String, Long, Array[String], Array[String]), String] {
  override def createAccumulator(): (String, Long, Array[String], Array[String]) =
    ("", 0, Array(), Array())

  override def add(value: WikipediaEditEvent, accumulator: (String, Long, Array[String], Array[String])): (String, Long, Array[String], Array[String]) =
    (value.getUser, 1, Array(value.getDiffUrl), Array(value.getSummary))

  override def merge(a: (String, Long, Array[String], Array[String]), b: (String, Long, Array[String], Array[String])): (String, Long, Array[String], Array[String]) =
    (a._1, a._2 + b._2, a._3 ++ b._3, a._4 ++ b._4)

  override def getResult(accumulator: (String, Long, Array[String], Array[String])): String = {
    implicit val Formats = DefaultFormats
    write(accumulator)
  }
}
