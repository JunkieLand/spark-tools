package it.trenzalore.spark.source.writer

import it.trenzalore.spark.source.SourceConfig
import org.apache.spark.sql.{ DataFrame, Dataset }

import scala.reflect.runtime.universe.TypeTag

/**
  * ParquetWriter allows to save a dataset to a Parquet file according to a source described in a SourceConfig.
  */
object ParquetWriter extends SourceWriter {

  /**
    * Save a dataset to a Parquet file according to a source described in a SourceConfig.
    * <br/>
    * <br/>
    * Since a DataFrame is just a Dataset[Row], it works for DataFrame too :)
    *
    * @param ds The dataset to be saved
    * @param sourceConfig A source description
    * @tparam T
    */
  override def save[T](ds: Dataset[T], sourceConfig: SourceConfig): Unit = {
    ds
      .write
      .options(sourceConfig.writeOptions)
      .partitionBy(sourceConfig.partitions: _*)
      .mode(sourceConfig.saveMode.get.toSparkSaveMode)
      .parquet(sourceConfig.path)
  }

}
