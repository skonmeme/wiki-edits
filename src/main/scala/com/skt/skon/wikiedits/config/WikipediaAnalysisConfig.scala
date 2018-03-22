package com.skt.skon.wikiedits.config

import scopt.OptionParser

case class WikipediaAnalysisConfig(sessionGapInMillis: Long = 2000,
                                   brokers: Seq[String] = Seq(),
                                   topicSummary: String = "",
                                   topicContents: String = "",
                                   groupId: String = "",
                                   sourceTasks: Int = -1,
                                   windowTasks: Int = -1,
                                   sinkTasks: Int = -1,
                                   autoWatermarkInterval: Long = 1000L,
                                   maxOutOfOrderness: Long = 1000L,
                                   kafkaMaxRequestSize: Long = 1048576,
                                   kafkaTransactionMaxTimeout: Long = 900000,
                                   checkpointStateBackend: StateBackend = NoStateBackend(),
                                   checkpointDataUri: String = "",
                                   checkpointInterval: Long = -1,
                                   sessionTimeout: Long = 60
                                  )

object WikipediaAnalysisConfig {
  def get(args: Array[String], programName: String): WikipediaAnalysisConfig = {
    val parser = new OptionParser[WikipediaAnalysisConfig](programName) {
      head(programName)

      help("help").text("prints this usage text")

      opt[Long]('s', "session-gap")
        .required()
        .action((x, c) => c.copy(sessionGapInMillis = x))
        .validate(x => if (x > 0) success else failure(s"session-gap must be positive but $x"))
        .text("Session gap in milliseconds")

      opt[Seq[String]]('b', "brokers")
        .required()
        .valueName("<addr:port>,<addr:port>...")
        .action((x, c) => c.copy(brokers = x))
        .text("List of Kafka brokers")

      opt[String]('t', "topic-summary")
        .required()
        .action((x, c) => c.copy(topicSummary = x))
        .text("Kafka topic for Summary")

      opt[String]('g', "group-id")
        .required()
        .action((x, c) => c.copy(groupId = x))
        .text("Kafka consumer group id")

      opt[String]('c', "topic-contents")
        .action((x, c) => c.copy(topicContents = x))
        .text("Kafka topic for Contents")

      opt[Int]("source-tasks")
        .action((x, c) => c.copy(sourceTasks = x))
        .text("# source tasks (recommendation : # partitions of input topic)")

      opt[Int]("window-tasks")
        .action((x, c) => c.copy(windowTasks = x ))
        .text("# window tasks")

      opt[Int]("sink-tasks")
        .action((x, c) => c.copy(sinkTasks = x))
        .text("# sink tasks")

      opt[Long]("auto-watermark-interval")
        .action((x, c) => c.copy(autoWatermarkInterval = x))
        .validate(x => if (x > 0) success else failure(s"auto-watermark-interval must be positive but $x"))

      opt[Long]("max-out-of-orderness")
        .action((x, c) => c.copy(maxOutOfOrderness = x))
        .validate(x => if (x > 0) success else failure(s"max-out-of-orderness must be positive but $x"))

      opt[Long]("kafka-max-request-size")
        .action((x, c) => c.copy(kafkaMaxRequestSize = x))
        .validate(x => if (x > 0) success else failure(s"kafka-max-request-size must be positive but $x"))

      opt[Long]("kafka-transaction-max-timeout")
        .action((x, c) => c.copy(kafkaTransactionMaxTimeout = x))
        .validate(x => if (x > 0) success else failure(s"kafka-transaction-max-timeout must be positive but $x"))

      opt[Long]("session-timeout")
        .action((x, c) => c.copy(sessionTimeout = x))
        .validate(x => if (x > 0) success else failure(s"session-timeout must be positive but $x"))
        .text("collection timeout (minute)")

      opt[String]("checkpoint-data-uri")
        .action((x, c) => c.copy(checkpointDataUri = x))

      opt[String]("checkpoint-state-backend")
        .action { (x, c) =>
          val stateBackend = x.toLowerCase match {
            case "memory" => MemoryStateBackend()
            case "fs" => FsStateBackend(c.checkpointDataUri)
            case "rocksdb" => RocksDBStateBackend(c.checkpointDataUri)
            case _ => NoStateBackend()
          }
          c.copy(checkpointStateBackend = stateBackend)
        }
        .validate { x =>
          x.toLowerCase match {
            case "memory" | "fs" | "rocksdb" => success
            case _ => failure(s"Unknown state backend : $x")
          }
        }

      opt[Long]("checkpoint-interval")
        .action((x, c) => c.copy(checkpointInterval = x))
        .validate(x => if (x > 0) success else failure(s"checkpoint-interval must be positive but $x"))
    }

    parser.parse(args, WikipediaAnalysisConfig()) match {
      case Some(c) => c
      case None => throw new RuntimeException("Failed to get a valid configuration object")
    }
  }
}