package it.trenzalore.tools.spark.source.writer

import it.trenzalore.tools.spark.source.SourceConfig
import it.trenzalore.tools.utils.logging.Logging
import org.apache.spark.sql.Dataset

/**
  * CsvWriter allows to save a dataset to a CSV file according to a source described in a SourceConfig.
  */
object CsvWriter extends SourceWriter with Logging {

  /**
    * Save a dataset to a CSV file according to a source described in a SourceConfig.
    * * 'delimiter' and 'header' have to be provided in the SourceConfig.
    * <br/>
    * <br/>
    * Since a DataFrame is just a Dataset[Row], it works for DataFrame too :)
    *
    * @param ds The dataset to be saved
    * @param sourceConfig A source description
    * @tparam T
    * @throws IllegalArgumentException If 'delimiter' or 'header' are not provided
    */
  def save[T](ds: Dataset[T], sourceConfig: SourceConfig): Unit = {
    logger.info(s"Will save dataset in CSV with the following configuration : $sourceConfig")

    if (sourceConfig.delimiter.isEmpty || sourceConfig.header.isEmpty)
      throw new IllegalArgumentException("'delimiter' and 'header' should be provided to write CSV")

    ds
      .write
      .options(sourceConfig.writeOptions)
      .partitionBy(sourceConfig.partitions: _*)
      .option("delimiter", sourceConfig.delimiter.get)
      .option("header", sourceConfig.header.get.toString)
      .mode(sourceConfig.writeStrategy.get.toSparkSaveMode)
      .csv(sourceConfig.path)
  }

}
