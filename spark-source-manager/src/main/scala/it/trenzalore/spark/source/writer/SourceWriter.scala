package it.trenzalore.spark.source.writer

import it.trenzalore.spark.source.{ FileFormat, SourceConfig }
import org.apache.spark.sql.Dataset

trait SourceWriter {

  def save[T](ds: Dataset[T], sourceConfig: SourceConfig): Unit

}

object SourceWriter {

  def getWriter(fileFormat: FileFormat): SourceWriter = fileFormat match {
    case FileFormat.CSV     ⇒ CsvWriter
    case FileFormat.Json    ⇒ JsonWriter
    case FileFormat.Parquet ⇒ ParquetWriter
    case FileFormat.ORC     ⇒ OrcWriter
  }

}