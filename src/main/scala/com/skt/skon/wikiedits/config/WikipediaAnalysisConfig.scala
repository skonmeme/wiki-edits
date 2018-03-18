package com.skt.skon.wikiedits.config

import scopt.OptionParser

case class WikipediaAnalysisConfig(sessionGapInMillis: Long = 2000,
                                   brokers: Seq[String] = Seq(),
                                   topic: String = "",
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
                                   checkpointInterval: Long = -1
                                  )

object WikipediaAnalysisConfig {
  def get(args: Array[String], programName: String): WikipediaAnalysisConfig = {
    val parser = new OptionParser[WikipediaAnalysisConfig](programName) {
      head(programName)

      help("help").text("prints this usage text")

      opt[Long]("sessionGap")
        .required()
        .action((x, c) => c.copy(sessionGapInMillis = x))
        .validate(x => if (x > 0) success else failure(s"sessionGap must be positive but $x"))
        .text("Session gap in milliseconds")

      opt[Seq[String]]("brokers")
        .required()
        .valueName("<addr:port>,<addr:port>...")
        .action((x, c) => c.copy(brokers = x))
        .text("List of Kafka brokers")

      opt[String]("topic")
        .required()
        .action((x, c) => c.copy(topic = x))
        .text("Kafka topic")

      opt[String]("groupId")
        .required()
        .action((x, c) => c.copy(groupId = x))
        .text("Kafka consumer group id")

      opt[Int]("sourceTasks")
        .action((x, c) => c.copy(sourceTasks = x))
        .text("# source tasks (recommendation : # partitions of input topic)")

      opt[Int]("windowTasks")
        .action((x, c) => c.copy(windowTasks = x ))
        .text("# window tasks")

      opt[Int]("sinkTasks")
        .action((x, c) => c.copy(sinkTasks = x))
        .text("# sink tasks")

      opt[Long]("autoWatermarkInterval")
        .action((x, c) => c.copy(autoWatermarkInterval = x))
        .validate(x => if (x > 0) success else failure(s"autoWatermarkInterval must be positive but $x"))

      opt[Long]("maxOutOfOrderness")
        .action((x, c) => c.copy(maxOutOfOrderness = x))
        .validate(x => if (x > 0) success else failure(s"maxOutOfOrderness must be positive but $x"))

      opt[Long]("kafkaMaxRequestSize")
        .action((x, c) => c.copy(kafkaMaxRequestSize = x))
        .validate(x => if (x > 0) success else failure(s"kafkaMaxRequestSize must be positive but $x"))

      opt[Long]("kafkaTransactionMaxTimeout")
        .action((x, c) => c.copy(kafkaTransactionMaxTimeout = x))
        .validate(x => if (x > 0) success else failure(s"kafkaTransactionMaxTimeout must be positive but $x"))

      opt[String]("checkpointDataUri")
        .action((x, c) => c.copy(checkpointDataUri = x))

      opt[String]("checkpointStateBackend")
        .action { (x, c) =>
          val stateBackend = x.toLowerCase match {
            case "memory" => MemoryStateBackend()
            case "fs" => FsStateBackend(c.checkpointDataUri)
            case "rocksdb" => RocksDBStateBackend(c.checkpointDataUri)
          }
          c.copy(checkpointStateBackend = stateBackend)
        }
        .validate { x =>
          x.toLowerCase match {
            case "memory" | "fs" | "rocksdb" => success
            case _ => failure(s"Unknown state backend : $x")
          }
        }

      opt[Long]("checkpointInterval")
        .action((x, c) => c.copy(checkpointInterval = x))
        .validate(x => if (x > 0) success else failure(s"checkpointInterval must be positive but $x"))
    }

    parser.parse(args, WikipediaAnalysisConfig()) match {
      case Some(c) => c
      case None => throw new RuntimeException("Failed to get a valid configuration object")
    }
  }
}