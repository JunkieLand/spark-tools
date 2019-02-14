package it.trenzalore.spark.source

import com.typesafe.config.Config
import it.trenzalore.utils.logging.Logging
import net.ceedubs.ficus.Ficus._

/**
  * SourceManager parses the 'sources' field in a configuration and instanciate SourceIO for each data source.
  * To use it :
  * <br/>
  * <br/>
  * val config = ConfigFactory.load()
  * <br/>
  * val sources = new SourceManager(config)
  * <br/>
  * val mySourceDs = sources("mySource").loadDs[MySource]()
  * <br/>
  * sources("mySource").save(ds)
  *
  * @param config A root Typesafe configuration
  */
class SourceManager(config: Config) {

  import SourceManager._

  val sources: Map[String, SourceIO] = parseSourceConfigs(config).map { case (k, v) ⇒ k -> new SourceIO(k, v) }

  def apply(sourceName: String): SourceIO = sources(sourceName)

}

object SourceManager extends Logging {

  def parseSourceConfigs(config: Config): Map[String, SourceConfig] = {
    if (!config.hasPath("sources"))
      throw new IllegalArgumentException("SourceManager expects a 'sources' entry in the configuration")

    config.as[Map[String, Config]]("sources").mapValues(SourceConfig.apply)
  }

}