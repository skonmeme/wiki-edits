package com.skt.skon.wikiedits.metrics

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter

class WikipeditEditSummaryMapper extends RichMapFunction[String, String] {
  @transient private var counter: Counter = _

  override def open(parameters: Configuration): Unit = {
    counter = getRuntimeContext
      .getMetricGroup
      .addGroup("WikipediaEdits")
      .counter("summaries")
  }

  override def map(value: String): String = {
    counter.inc()
    value
  }

}

class WikipeditEditContentsMapper extends RichMapFunction[String, String] {
  @transient private var counter: Counter = _

  override def open(parameters: Configuration): Unit = {
    counter = getRuntimeContext
      .getMetricGroup
      .addGroup("WikipediaEdits")
      .counter("contents")
  }

  override def map(value: String): String = {
    counter.inc()
    value
  }

}
