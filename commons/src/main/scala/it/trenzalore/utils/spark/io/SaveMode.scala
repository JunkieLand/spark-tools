package it.trenzalore.utils.spark.io
import enumeratum.{ Enum, EnumEntry }

sealed trait SaveMode extends EnumEntry

object SaveMode extends Enum[SaveMode] {

  val values = findValues

  case object Overwrite extends SaveMode
  case object Append extends SaveMode
  case object Ignore extends SaveMode
  case object ErrorIfExists extends SaveMode
  case object OverwriteWhenSuccessful extends SaveMode

  def apply(str: String): SaveMode = withNameInsensitive(str)

}
