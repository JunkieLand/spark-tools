package it.trenzalore.spark.source.reader

import it.trenzalore.spark.source.{ FileFormat, SourceConfig }
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }

import scala.reflect.runtime.universe.TypeTag

trait SourceReader {

  def loadDf(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataFrame

  def loadDs[T <: Product: TypeTag](sourceConfig: SourceConfig)(implicit spark: SparkSession): Dataset[T]

}

object SourceReader {

  def getReader(fileFormat: FileFormat): SourceReader = fileFormat match {
    case FileFormat.CSV     ⇒ CsvReader
    case FileFormat.Json    ⇒ JsonReader
    case FileFormat.Parquet ⇒ ParquetReader
    case FileFormat.ORC     ⇒ OrcReader
  }

}