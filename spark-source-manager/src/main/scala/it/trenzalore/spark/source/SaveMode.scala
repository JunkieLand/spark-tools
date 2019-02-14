package it.trenzalore.spark.source

import scala.collection.immutable
import enumeratum.Enum
import enumeratum.EnumEntry
import org.apache.spark.sql

sealed trait SaveMode extends EnumEntry {

  def toSparkSaveMode: org.apache.spark.sql.SaveMode

}

/**
  * Like the original Spark SaveMode, it is used to specify the write strategy when saving a dataset. However,
  * it adds a few nice strategies.
  */
object SaveMode extends Enum[SaveMode] {

  val values: immutable.IndexedSeq[SaveMode] = findValues

  /**
    * Overwrite mode means that if any data exists in the directory, it will be deleted and overwritten by the new one.
    * This Overwrite mode has exactly the same behavior than the Spark SaveMode.Overwrite mode. Hence, the old data
    * will be deleted <strong>before</strong> starting writing the new one, so data will be unavailable until finishing to write the
    * new one.
    */
  case object Overwrite extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = sql.SaveMode.Overwrite
  }

  /**
    * Append mode means that if any data exists in the directory, the new data will be appended leaving the old data
    * untouched. This Append has exactly the same behavior than the Spark SaveMode.Append mode.
    */
  case object Append extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = sql.SaveMode.Append
  }

  /**
    * Ignore mode means that if any data exists in the directory, nothing new will be written leaving the old data
    * untouched. This Ignore has exactly the same behavior than the Spark SaveMode.Ignore mode.
    */
  case object Ignore extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = sql.SaveMode.Ignore
  }

  /**
    * ErrorIfExists means that if any data exists in the directory, nothing new will be written and an exception
    * will be thrown. This ErrorIfExists has exactly the same behavior than the Spark SaveMode.ErrorIfExists mode.
    */
  case object ErrorIfExists extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = sql.SaveMode.ErrorIfExists
  }

  /**
    * OverwriteWhenSuccessful mode means that if any data exists in the directory, it will be deleted and overwritten
    * by the new one. However, contrary to the Overwrite mode, the new data will be first written in a temporary
    * directory, then <strong>after</strong> writing is finished, the old data will be deleted and the new one moved to its place.
    * The unavailability duration is thus reduced to almost nothing.
    */
  case object OverwriteWhenSuccessful extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = throw new IllegalAccessException("OverwriteWhenSuccessful has no Spark equivalent save mode")
  }

  /**
    * OverwritePartitions means that if partitioned data exists in the directory, when writing new partitioned
    * data, only old partitions present in the new data will be overwritten. The other partitions will be left
    * untouched.
    * <br/>
    * <br/>
    * Be careful when writing many different partitions since this works by writing first in a temporary directory,
    * then deleting each matching old partition and moving the new one. It might over stress the name node.
    */
  case object OverwritePartitions extends SaveMode {
    def toSparkSaveMode: sql.SaveMode = throw new IllegalAccessException("OverwriteWhenSuccessful has no Spark equivalent save mode")
  }

  def apply(str: String): SaveMode = withNameInsensitive(str)

}
