package com.skt.skon.wikiedits.aggregator

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s._
import org.json4s.ext._
import org.json4s.native.Serialization._

case class WikipediaEditSummary(user: String,
                                start_at: DateTime,
                                count: Long,
                                bytes: Long)

case class WikipediaEditContents(user: String,
                                 created_at: DateTime,
                                 title: String,
                                 channel: String,
                                 summaries: String,
                                 url: String)

class WikipediaEditEventSummaryAggregate extends AggregateFunction[WikipediaEditEvent, (String, DateTime, Long, Long), String] {
  override def createAccumulator(): (String, DateTime, Long, Long) =
    ("", null, 0L, 0L)

  override def add(value: WikipediaEditEvent, accumulator: (String, DateTime, Long, Long)) : (String, DateTime, Long, Long) =
    (value.getUser, new DateTime(value.getTimestamp, DateTimeZone.UTC), 1, value.getByteDiff + accumulator._4)

  override def merge(a: (String, DateTime, Long, Long), b: (String, DateTime, Long, Long)): (String, DateTime, Long, Long) =
    (a._1, a._2, a._3 + b._3, a._4 + b._4)

  override def getResult(accumulator: (String, DateTime, Long, Long)): String = {
    implicit val Formats = DefaultFormats ++ JodaTimeSerializers.all
    write(WikipediaEditSummary.tupled(accumulator))
  }
}

class WikipediaEditEventContentsAggregate extends AggregateFunction[WikipediaEditEvent, WikipediaEditEvent, String] {
  override def createAccumulator(): WikipediaEditEvent = null

  override def add(value: WikipediaEditEvent, accumulator: WikipediaEditEvent): WikipediaEditEvent = value

  override def merge(accumulator1: WikipediaEditEvent, accumulator2: WikipediaEditEvent): WikipediaEditEvent = accumulator2

  override def getResult(accumulator: WikipediaEditEvent): String = {
    implicit val Formats = DefaultFormats ++ JodaTimeSerializers.all
    write(WikipediaEditContents.tupled((
      accumulator.getUser,
      new DateTime(accumulator.getTimestamp, DateTimeZone.UTC),
      accumulator.getTitle,
      accumulator.getChannel,
      accumulator.getSummary,
      accumulator.getDiffUrl
    )))
  }
}
