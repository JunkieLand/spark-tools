package it.trenzalore.utils.spark.io

import it.trenzalore.utils.logging.Logging
import it.trenzalore.utils.spark.FileFormat
import it.trenzalore.utils.spark.SparkUtils._
import it.trenzalore.utils.spark.SparkUtils.implicits._
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ DataFrame, DataFrameWriter, Dataset, SparkSession }

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

class SourceIO(sourceName: String, sourceConfig: SourceConfig) extends Logging {

  def loadDs[T <: Product: TypeTag]()(implicit spark: SparkSession): Dataset[T] = {
    sourceConfig.format match {
      case FileFormat.Json ⇒ loadJsonDs[T]()
      case FileFormat.CSV  ⇒ loadCsvDs[T]()
      case _               ⇒ loadDf().to[T]
    }
  }

  def loadDf()(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Loading source '$sourceName' with configuration '$sourceConfig'")
    sourceConfig.format match {
      case FileFormat.CSV     ⇒ loadCsvDf()
      case FileFormat.Json    ⇒ loadJsonDf()
      case FileFormat.ORC     ⇒ loadOrc()
      case FileFormat.Parquet ⇒ loadParquet()
    }
  }

  def save[T <: Product: TypeTag](ds: Dataset[T])(implicit fs: FileSystem): Unit = {
    logger.info(s"Saving source '$sourceName' with configuration '$sourceConfig'")

    val ds1 = ds
      .write
      .format(sourceConfig.format.toString.toLowerCase)
      .options(sourceConfig.writeOptions + ("path" -> sourceConfig.path)) // Necessary to create an EXTERNAL table
      .partitionBy(sourceConfig.partitions: _*)

    val ds2 = if (sourceConfig.format == FileFormat.CSV) {
      ds1.option("delimiter", sourceConfig.delimiter.get).option("header", sourceConfig.header.get.toString)
    } else {
      ds1
    }

    sourceConfig.saveMode.get match {
      case SaveMode.Overwrite               ⇒ ds2.mode(org.apache.spark.sql.SaveMode.Overwrite).save()
      case SaveMode.Append                  ⇒ ds2.mode(org.apache.spark.sql.SaveMode.Append).save()
      case SaveMode.Ignore                  ⇒ ds2.mode(org.apache.spark.sql.SaveMode.Ignore).save()
      case SaveMode.ErrorIfExists           ⇒ ds2.mode(org.apache.spark.sql.SaveMode.ErrorIfExists).save()
      case SaveMode.OverwriteWhenSuccessful ⇒ saveWithTmpDir(ds)
    }

    //    if (sourceConfig.createExternalTable) {
    //      dataframeWriter(ds).saveAsTable(sourceConfig.table.get)
    //    } else {
    //      dataframeWriter(ds).save()
    //    }
  }

  def saveWithTmpDir[T <: Product: TypeTag](ds: Dataset[T])(implicit fs: FileSystem): Unit = {
    val tmpSourceConfig = sourceConfig.copy(path = sourceConfig.path + "_" + Random.nextString(20))

    val ds1 = ds
      .write
      .format(tmpSourceConfig.format.toString.toLowerCase)
      .options(tmpSourceConfig.writeOptions + ("path" -> tmpSourceConfig.path)) // Necessary to create an EXTERNAL table
      .partitionBy(tmpSourceConfig.partitions: _*)

    val ds2 = if (tmpSourceConfig.format == FileFormat.CSV) {
      ds1.option("delimiter", tmpSourceConfig.delimiter.get).option("header", tmpSourceConfig.header.get.toString)
    } else {
      ds1
    }

    ds2.mode(org.apache.spark.sql.SaveMode.Overwrite).save()

    fs.delete(new Path(sourceConfig.path), true)
    fs.rename(new Path(tmpSourceConfig.path), new Path(sourceConfig.path))
  }

  def delete()(implicit fs: FileSystem): Boolean = {
    logger.info(s"Deleting directory ${sourceConfig.path} for source '$sourceName'")
    fs.delete(new Path(sourceConfig.path), true)
  }

  private def loadJsonDs[T <: Product: TypeTag]()(implicit spark: SparkSession): Dataset[T] = {
    spark.read.options(sourceConfig.readOptions).schema(schemaOf[T]).json(sourceConfig.path).to[T]
  }

  private def loadJsonDf()(implicit spark: SparkSession): DataFrame = {
    spark.read.options(sourceConfig.readOptions).json(sourceConfig.path)
  }

  private def loadCsvDs[T <: Product: TypeTag]()(implicit spark: SparkSession): Dataset[T] = {
    spark
      .read
      .options(sourceConfig.readOptions)
      .option("delimiter", sourceConfig.delimiter.get)
      .option("header", sourceConfig.header.get)
      .schema(schemaOf[T])
      .csv(sourceConfig.path)
      .to[T]
  }

  private def loadCsvDf()(implicit spark: SparkSession): DataFrame = {
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
    sourceConfig.saveMode.get match {
      case SaveMode.Overwrite ⇒
        ds
          .write
          .format(sourceConfig.format.toString.toLowerCase)
          .mode(org.apache.spark.sql.SaveMode.Overwrite)
          .options(sourceConfig.writeOptions + ("path" -> sourceConfig.path)) // Necessary to create an EXTERNAL table
          .partitionBy(sourceConfig.partitions: _*)
    }

  }

}

