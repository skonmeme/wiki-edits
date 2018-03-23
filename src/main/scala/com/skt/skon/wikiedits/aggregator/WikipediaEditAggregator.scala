package com.skt.skon.wikiedits.aggregator

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent
import org.joda.time.DateTime
import org.json4s._
import org.json4s.native.Serialization._

case class WikipediaEditSummary(user: String,
                                start_at: DateTime,
                                count: Long,
                                bytes: Long)

case class WikipediaEditContents(user: String,
                                 count: Long,
                                 urls: Array[String],
                                 summaries: Array[String],
                                 created_at: Array[DateTime])

class WikipediaEditEventSummaryAggregate extends AggregateFunction[WikipediaEditEvent, (String, DateTime, Long, Long), String] {
  override def createAccumulator(): (String, DateTime, Long, Long) =
    ("", null, 0L, 0L)

  override def add(value: WikipediaEditEvent, accumulator: (String, DateTime, Long, Long)) =
    (value.getUser, new DateTime(value.getTimestamp), 1, value.getByteDiff + accumulator._4)

  override def merge(a: (String, DateTime, Long, Long), b: (String, DateTime, Long, Long)): (String, DateTime, Long, Long) =
    (a._1, a._2, a._3 + b._3, a._4 + b._4)

  override def getResult(accumulator: (String, DateTime, Long, Long)): String = {
    implicit val Formats = DefaultFormats
    write(WikipediaEditSummary.tupled(accumulator))
  }
}

class WikipediaEditEventContentsAggregate extends AggregateFunction[WikipediaEditEvent, (String, Long, Array[String], Array[String], Array[DateTime]), String] {
  override def createAccumulator(): (String, Long, Array[String], Array[String], Array[DateTime]) =
    ("", 0, Array(), Array(), Array())

  override def add(value: WikipediaEditEvent, accumulator: (String, Long, Array[String], Array[String], Array[DateTime])): (String, Long, Array[String], Array[String], Array[DateTime]) =
    (value.getUser, 1, Array(value.getDiffUrl), Array(value.getSummary), Array(new DateTime(value.getTimestamp)))

  override def merge(a: (String, Long, Array[String], Array[String], Array[DateTime]), b: (String, Long, Array[String], Array[String], Array[DateTime])): (String, Long, Array[String], Array[String], Array[DateTime]) =
    (a._1, a._2 + b._2, a._3 ++ b._3, a._4 ++ b._4, a._5 ++ a._5)

  override def getResult(accumulator: (String, Long, Array[String], Array[String], Array[DateTime])): String = {
    implicit val Formats = DefaultFormats
    write(WikipediaEditContents.tupled(accumulator))
  }
}
