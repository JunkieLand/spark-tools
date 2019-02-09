package it.trenzalore.utils.spark.io

import com.typesafe.config.Config
import it.trenzalore.utils.spark.FileFormat
import net.ceedubs.ficus.Ficus._

case class SourceConfig(
  path:      String,
  format:    FileFormat,
  delimiter: Option[String] = None,
  header:    Option[String] = None
)

object SourceConfig {

  def apply(config: Config): SourceConfig = SourceConfig(
    path = config.getString("path"),
    format = FileFormat(config.getString("format")),
    delimiter = config.as[Option[String]]("delimiter"),
    header = config.as[Option[String]]("header")
  )

}
