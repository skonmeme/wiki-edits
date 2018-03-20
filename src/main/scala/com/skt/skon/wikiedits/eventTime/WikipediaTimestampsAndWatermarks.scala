package com.skt.skon.wikiedits.eventTime

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent

class WikipediaTimestampsAndWatermarks extends AssignerWithPeriodicWatermarks[WikipediaEditEvent] {

  val maxOutOfOrderness = 1000L   // 1  seconds
  val maxBound = 10000L           // 10 seconds

  var currentMaxTimestamp = 0L

  override def extractTimestamp(event: WikipediaEditEvent, l: Long): Long = {
    val timestamp = event.getTimestamp
    currentMaxTimestamp = math.max(timestamp, currentMaxTimestamp)
    timestamp
  }

  override def getCurrentWatermark: Watermark = {
    new Watermark(math.min(currentMaxTimestamp - maxOutOfOrderness, System.currentTimeMillis() - maxBound))
  }

}
