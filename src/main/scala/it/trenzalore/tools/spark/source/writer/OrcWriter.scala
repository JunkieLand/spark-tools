package it.trenzalore.tools.spark.source.writer

import it.trenzalore.tools.spark.source.SourceConfig
import it.trenzalore.tools.utils.logging.Logging
import org.apache.spark.sql.Dataset

/**
  * OrcWriter allows to save a dataset to a ORC file according to a source described in a SourceConfig.
  */
object OrcWriter extends SourceWriter with Logging {

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
    logger.info(s"Will save dataset in Orc with the following configuration : $sourceConfig")

    ds
      .write
      .options(sourceConfig.writeOptions)
      .partitionBy(sourceConfig.partitions: _*)
      .mode(sourceConfig.writeStrategy.get.toSparkSaveMode)
      .orc(sourceConfig.path)
  }

}
