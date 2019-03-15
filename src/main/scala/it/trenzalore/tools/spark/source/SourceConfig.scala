package it.trenzalore.tools.spark.source

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * SourceConfig is a data source description allowing to read, save or delete it.
  *
  * @param path Location of the source directory. Mandatory.
  * @param format File format of the source files. Mandatory.
  * @param delimiter CSV delimiter character. Optional unless using CSV file format.
  * @param header Whether CSV files have a header or not. Optional unless using CSV file format.
  * @param partitions Partitioning fields when saving a dataset. Optional if only reading.
  * @param writeStrategy Write strategy when writing a dataset. Optional if only reading.
  * @param table External table name when writing a dataset. Optional if only reading.
  * @param createExternalTable Whether to create an external table when saving a dataset or not. Optional if only reading. Defaults to 'false'.
  * @param readOptions A Map of additional options to use when reading a dataset.
  * @param writeOptions A mAp of additional options to use when writing a dataset.
  */
case class SourceConfig(
  path:                String,
  format:              FileFormat,
  delimiter:           Option[String]        = None,
  header:              Option[Boolean]       = None,
  partitions:          Seq[String]           = Seq.empty,
  writeStrategy:       Option[WriteStrategy] = None,
  table:               Option[String]        = None,
  createExternalTable: Boolean,
  readOptions:         Map[String, String]   = Map(),
  writeOptions:        Map[String, String]   = Map()
)

object SourceConfig {

  def apply(config: Config): SourceConfig = SourceConfig(
    path = config.getString("path"),
    format = FileFormat(config.getString("format")),
    delimiter = config.as[Option[String]]("delimiter"),
    header = config.as[Option[Boolean]]("header"),
    partitions = config.as[Option[Vector[String]]]("partitions").getOrElse(Vector.empty),
    writeStrategy = config.as[Option[String]]("writeStrategy").map(WriteStrategy.apply),
    table = config.as[Option[String]]("table"),
    createExternalTable = config.as[Option[Boolean]]("createExternalTable").getOrElse(false),
    readOptions = config.as[Option[Map[String, String]]]("readOptions").getOrElse(Map.empty),
    writeOptions = config.as[Option[Map[String, String]]]("writeOptions").getOrElse(Map.empty)
  )

}
