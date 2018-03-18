package com.skt.skon.wikiedits.aggregator

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent

class WikipediaEditEventAggregate extends AggregateFunction[WikipediaEditEvent, (String, Long), (String, Long)] {
  override def createAccumulator(): (String, Long) =
    ("", 0L)

  override def add(value: WikipediaEditEvent, accumulator: (String, Long)) =
    (value.getUser, value.getByteDiff + accumulator._2)

  override def merge(a: (String, Long), b: (String, Long)): (String, Long) =
    (a._1, a._2 + b._2)

  override def getResult(accumulator: (String, Long)): (String, Long) =
    accumulator
}
