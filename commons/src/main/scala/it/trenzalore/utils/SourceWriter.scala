package it.trenzalore.utils

import com.typesafe.config.{ Config, ConfigFactory }
import it.trenzalore.utils.spark.FileFormat
import org.apache.spark.sql.{ Dataset, SaveMode }

import scala.collection.JavaConversions._

trait SourceWriter {

  val sourceName: String

  lazy val config: Config = ConfigFactory.load()

  lazy val sourceFormat: FileFormat = FileFormat(config.getString(s"sources.$sourceName.format"))

  lazy val sourcePath: String = config.getString(s"sources.$sourceName.path")

  lazy val database: String = config.getString(s"sources.$sourceName.database")

  lazy val table: String = config.getString(s"sources.$sourceName.table")

  lazy val options: Map[String, String] = {
    if (config.hasPath(s"sources.$sourceName.options")) {
      config
        .getConfig(s"sources.$sourceName.options")
        .entrySet()
        .map { entry ⇒ entry.getKey -> entry.getValue.unwrapped().asInstanceOf[String] }
        .toMap
    } else {
      Map.empty
    }
  }

  lazy val partitions: Seq[String] = {
    if (config.hasPath(s"sources.$sourceName.partitions")) {
      config.getStringList(s"sources.$sourceName.partitions")
    } else {
      Nil
    }
  }

  def save[T](ds: Dataset[T]): Unit = save(ds, SaveMode.ErrorIfExists)

  def save[T](ds: Dataset[T], mode: SaveMode): Unit = {
    ds
      .write
      .format(sourceFormat.toString.toLowerCase)
      .mode(mode)
      .options(options)
      .partitionBy(partitions: _*)
      .save(sourcePath)
  }

  def saveWithTable[T](ds: Dataset[T]): Unit = saveWithTable(ds, SaveMode.ErrorIfExists)

  def saveWithTable[T](ds: Dataset[T], mode: SaveMode): Unit = {
    ds
      .write
      .format(sourceFormat.toString.toLowerCase)
      .mode(mode)
      .options(options + ("path" -> sourcePath)) // Necessary to create an EXTERNAL table
      .partitionBy(partitions: _*)
      .saveAsTable(s"$database.$table")
  }

}

object SourceWriter {

  def apply(name: String): SourceWriter = new SourceWriter {
    val sourceName: String = name
  }

  def apply(name: String, configOverride: Config): SourceWriter = new SourceWriter {
    val sourceName: String = name
    override lazy val config: Config = configOverride
  }

}