package it.trenzalore.utils.spark.io.writer

import it.trenzalore.utils.spark.io.SourceConfig
import org.apache.spark.sql.{ DataFrame, Dataset }

import scala.reflect.runtime.universe.TypeTag

object ParquetWriter extends SourceWriter {

  override def save[T](ds: Dataset[T], sourceConfig: SourceConfig): Unit = {
    ds
      .write
      .options(sourceConfig.writeOptions)
      .partitionBy(sourceConfig.partitions: _*)
      .mode(sourceConfig.saveMode.get.toSparkSaveMode)
      .parquet(sourceConfig.path)
  }

}
