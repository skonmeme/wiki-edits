package com.skt.skon.wikiedits.aggregator

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent

class WikipediaEditEventSummaryAggregate extends AggregateFunction[WikipediaEditEvent, (String, Long, Long), String] {
  override def createAccumulator(): (String, Long, Long) =
    ("", 0, 0L)

  override def add(value: WikipediaEditEvent, accumulator: (String, Long, Long)) =
    (value.getUser, 1, value.getByteDiff + accumulator._2)

  override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) =
    (a._1, a._2 + b._2, a._3 + b._3)

  override def getResult(accumulator: (String, Long)): String =
    accumulator.toString // change to JSON coding
}

class WikipediaEditEventContentsAggregate extends AggregateFunction[WikipediaEditEvent, (String, String), String] {
  override def createAccumulator(): (String, String) =
    ("", "")

  override def add(value: WikipediaEditEvent, accumulator: (String, String)) =
    (value.getUser, value.getSummary)

  override def merge(a: (String, Long), b: (String, Long)): (String, Long) =
    (a._1, a._2 + b._2)

  override def getResult(accumulator: (String, Long)): String =
    accumulator.toString
}
