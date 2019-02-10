package it.trenzalore.utils.spark.io

import com.typesafe.config.Config
import it.trenzalore.utils.spark.FileFormat
import net.ceedubs.ficus.Ficus._

case class SourceConfig(
  path:                String,
  format:              FileFormat,
  delimiter:           Option[String]      = None,
  header:              Option[Boolean]     = None,
  partitions:          Seq[String]         = Seq.empty,
  table:               Option[String]      = None,
  saveMode:            Option[SaveMode]    = None,
  createExternalTable: Boolean,
  readOptions:         Map[String, String] = Map(),
  writeOptions:        Map[String, String] = Map()
)

object SourceConfig {

  def apply(config: Config): SourceConfig = SourceConfig(
    path = config.getString("path"),
    format = FileFormat(config.getString("format")),
    delimiter = config.as[Option[String]]("delimiter"),
    header = config.as[Option[Boolean]]("header"),
    partitions = config.as[Option[Vector[String]]]("partitions").getOrElse(Vector.empty),
    table = config.as[Option[String]]("table"),
    saveMode = config.as[Option[String]]("saveMode").map(SaveMode.apply),
    createExternalTable = config.as[Option[Boolean]]("createExternalTable").getOrElse(false),
    readOptions = config.as[Option[Map[String, String]]]("readOptions").getOrElse(Map.empty),
    writeOptions = config.as[Option[Map[String, String]]]("writeOptions").getOrElse(Map.empty)
  )

}
