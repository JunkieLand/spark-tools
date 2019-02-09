package it.trenzalore.utils.spark.io

import com.typesafe.config.Config
import it.trenzalore.utils.logging.Logging
import net.ceedubs.ficus.Ficus._

class SourceManager(config: Config) {

  import SourceManager._

  val sources: Map[String, SourceIO] = parseSourceConfigs(config).map { case (k, v) ⇒ k -> new SourceIO(k, v) }

  def apply(sourceName: String): SourceIO = sources(sourceName)

}

object SourceManager extends Logging {

  def parseSourceConfigs(config: Config): Map[String, SourceConfig] = {
    config.as[Map[String, Config]]("sources").mapValues(SourceConfig.apply)
  }

}