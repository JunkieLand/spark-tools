package it.trenzalore.utils.spark.io

import it.trenzalore.utils.logging.Logging
import it.trenzalore.utils.spark.FileFormat
import it.trenzalore.utils.spark.SparkUtils.implicits._
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ DataFrame, DataFrameWriter, Dataset, SparkSession }

import scala.reflect.runtime.universe.TypeTag

class SourceIO(sourceName: String, sourceConfig: SourceConfig) extends Logging {

  def loadDs[T <: Product: TypeTag]()(implicit spark: SparkSession): Dataset[T] = {
    loadDf().to[T]
  }

  def loadDf()(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Loading source '$sourceName' with configuration '$sourceConfig'")
    sourceConfig.format match {
      case FileFormat.CSV     ⇒ loadCsv()
      case FileFormat.Json    ⇒ loadJson()
      case FileFormat.ORC     ⇒ loadOrc()
      case FileFormat.Parquet ⇒ loadParquet()
    }
  }

  def save[T <: Product: TypeTag](ds: Dataset[T]): Unit = {
    logger.info(s"Saving source '$sourceName' with configuration '$sourceConfig'")
    if (sourceConfig.createExternalTable) {
      dataframeWriter(ds).saveAsTable(sourceConfig.table.get)
    } else {
      dataframeWriter(ds).save()
    }
  }

  def delete()(implicit fs: FileSystem): Boolean = {
    logger.info(s"Deleting directory ${sourceConfig.path} for source '$sourceName'")
    fs.delete(new Path(sourceConfig.path), true)
  }

  private def loadJson()(implicit spark: SparkSession): DataFrame = {
    spark.read.options(sourceConfig.readOptions).json(sourceConfig.path)
  }

  private def loadCsv()(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .options(sourceConfig.readOptions)
      .option("delimiter", sourceConfig.delimiter.get)
      .option("header", sourceConfig.header.get)
      .csv(sourceConfig.path)
  }

  private def loadOrc()(implicit spark: SparkSession): DataFrame = {
    spark.read.options(sourceConfig.readOptions).orc(sourceConfig.path)
  }

  private def loadParquet()(implicit spark: SparkSession): DataFrame = {
    spark.read.options(sourceConfig.readOptions).parquet(sourceConfig.path)
  }

  private def dataframeWriter[T](ds: Dataset[T]): DataFrameWriter[T] = {
    ds
      .write
      .format(sourceConfig.format.toString.toLowerCase)
      .mode(sourceConfig.saveMode.get)
      .options(sourceConfig.writeOptions + ("path" -> sourceConfig.path)) // Necessary to create an EXTERNAL table
      .partitionBy(sourceConfig.partitions: _*)
  }

}

