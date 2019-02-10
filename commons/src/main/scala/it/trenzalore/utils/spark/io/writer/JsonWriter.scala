package it.trenzalore.utils.spark.io.writer

import it.trenzalore.utils.spark.io.SourceConfig
import org.apache.spark.sql.{ DataFrame, Dataset }

import scala.reflect.runtime.universe.TypeTag

object JsonWriter extends SourceWriter {

  def save[T](ds: Dataset[T], sourceConfig: SourceConfig): Unit = {
    ds
      .write
      .options(sourceConfig.writeOptions)
      .partitionBy(sourceConfig.partitions: _*)
      .mode(sourceConfig.saveMode.get.toSparkSaveMode)
      .json(sourceConfig.path)
  }

}
