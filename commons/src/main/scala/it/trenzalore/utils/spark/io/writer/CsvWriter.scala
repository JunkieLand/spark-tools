package it.trenzalore.utils.spark.io.writer
import it.trenzalore.utils.spark.io.SourceConfig
import org.apache.spark.sql.Dataset

object CsvWriter extends SourceWriter {

  def save[T](ds: Dataset[T], sourceConfig: SourceConfig): Unit = {
    if (sourceConfig.delimiter.isEmpty || sourceConfig.header.isEmpty)
      throw new IllegalArgumentException("'delimiter' and 'header' should be provided to write CSV")

    ds
      .write
      .options(sourceConfig.writeOptions)
      .partitionBy(sourceConfig.partitions: _*)
      .option("delimiter", sourceConfig.delimiter.get)
      .option("header", sourceConfig.header.get.toString)
      .mode(sourceConfig.saveMode.get.toSparkSaveMode)
      .csv(sourceConfig.path)
  }

}
