package it.trenzalore.utils.spark.io
import enumeratum.{ Enum, EnumEntry }
import org.apache.spark.sql

sealed trait SaveMode extends EnumEntry {

  def toSparkSaveMode: org.apache.spark.sql.SaveMode

}

object SaveMode extends Enum[SaveMode] {

  val values = findValues

  case object Overwrite extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = sql.SaveMode.Overwrite
  }
  case object Append extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = sql.SaveMode.Append
  }
  case object Ignore extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = sql.SaveMode.Ignore
  }
  case object ErrorIfExists extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = sql.SaveMode.ErrorIfExists
  }
  case object OverwriteWhenSuccessful extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = throw new IllegalAccessException("OverwriteWhenSuccessful has no Spark equivalent save mode")
  }
  case object OverwritePartitions extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = throw new IllegalAccessException("OverwriteWhenSuccessful has no Spark equivalent save mode")
  }

  def apply(str: String): SaveMode = withNameInsensitive(str)

}
