package it.trenzalore.utils.spark

import enumeratum.{ Enum, EnumEntry }

sealed trait FileFormat extends EnumEntry

object FileFormat extends Enum[FileFormat] {
  override val values: scala.collection.immutable.IndexedSeq[FileFormat] = findValues
  case object Json extends FileFormat
  case object CSV extends FileFormat
  case object ORC extends FileFormat
  case object Parquet extends FileFormat
  def apply(str: String): FileFormat = withNameInsensitive(str)
}