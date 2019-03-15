package it.trenzalore.tools.spark.source.writer

import it.trenzalore.tools.spark.source.SourceConfig
import org.apache.spark.sql.{ DataFrame, Dataset }

import scala.reflect.runtime.universe.TypeTag

/**
  * OrcWriter allows to save a dataset to a ORC file according to a source described in a SourceConfig.
  */
object OrcWriter extends SourceWriter {

  /**
    * Save a dataset to a ORC file according to a source described in a SourceConfig.
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
      .mode(sourceConfig.writeStrategy.get.toSparkSaveMode)
      .orc(sourceConfig.path)
  }

}
