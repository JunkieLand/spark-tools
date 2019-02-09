package it.trenzalore.utils.spark

import com.typesafe.config.Config
import it.trenzalore.utils.SourceWriter
import it.trenzalore.utils.logging.Logging
import it.trenzalore.utils.spark.SparkUtils.implicits._
import it.trenzalore.utils.spark.io.SourceConfig
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ DataFrame, Dataset, SaveMode, SparkSession }

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe.TypeTag

case class SourceManager(config: Config) extends Logging {
  import SourceManager._

  val sourceConfigs: Map[String, SourceConfig] = parseSourceConfigs(config)

  def pathOf(sourceName: String) = sourceConfigs(sourceName).path

  //  def formatOf(sourceName: String) = sourceConfigs(sourceName).format

  def delete(sourceName: String)(implicit fs: FileSystem) = {
    val dir = pathOf(sourceName)
    logger.info(s"Deleting directory '$dir'")
    fs.delete(new Path(dir), true)
  }

  def save[T <: Product: TypeTag](sourceName: String, ds: Dataset[T], saveMode: SaveMode) = {
    SourceWriter(sourceName, config).saveWithTable(ds, saveMode)
  }

  def loadDs[T <: Product: TypeTag](sourceName: String)(implicit spark: SparkSession): Dataset[T] = {
    loadDf(sourceName).to[T]
  }

  def loadDf(sourceName: String)(implicit spark: SparkSession): DataFrame = {
    val sourceConfig = sourceConfigs(sourceName)
    sourceConfig.format match {
      case FileFormat.CSV     ⇒ loadCsv(sourceConfig)
      case FileFormat.Json    ⇒ loadJson(sourceConfig)
      case FileFormat.ORC     ⇒ loadOrc(sourceConfig)
      case FileFormat.Parquet ⇒ loadParquet(sourceConfig)
    }
  }

  private def loadJson(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    spark.read.json(sourceConfig.path)
  }

  private def loadCsv(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .option("delimiter", sourceConfig.delimiter.get)
      .option("header", sourceConfig.header.get)
      .csv(sourceConfig.path)
  }

  private def loadOrc(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    spark.read.orc(sourceConfig.path)
  }

  private def loadParquet(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(sourceConfig.path)
  }

}

object SourceManager extends Logging {

  def parseSourceConfigs(config: Config): Map[String, SourceConfig] = {
    val sourceConfigs = config.getConfig("sources")
    val sourceNames = sourceConfigs.entrySet().map(_.getKey.split('.').head)

    val sourceConfigMap = sourceNames.map { sourceName ⇒
      sourceName -> SourceConfig(sourceConfigs.getConfig(sourceName))
    }.toMap

    logger.info(s"Successfully parsed source configuration $sourceConfigMap")

    sourceConfigMap
  }

}