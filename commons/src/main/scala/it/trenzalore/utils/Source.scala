package it.trenzalore.utils

import it.trenzalore.utils.spark.SparkUtils.implicits._
import com.typesafe.config.{ Config, ConfigFactory }
import it.trenzalore.utils.spark.FileFormat
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.sql.{ DataFrame, Dataset, Encoders, SparkSession }

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

/**
  * Representation of a source
  */

trait Source {

  val sourceName: String

  val config: Config

  lazy val sourceFormat: FileFormat = FileFormat(Try(config.getString(s"sources.$sourceName.format")) getOrElse "parquet")

  lazy val sourcePath: String = config.getString(s"sources.$sourceName.path")

  def load(implicit spark: SparkSession): DataFrame = load(sourcePath)

  def loadTyped[T <: Product: TypeTag](config: Config)(implicit spark: SparkSession): Dataset[T] = {
    val df = load(config)
    toDsWithSpecificColumns[T](df)
  }

  protected def load(path: String)(implicit spark: SparkSession): DataFrame = sourceFormat match {
    case FileFormat.Json    ⇒ spark.read.json(path)
    case FileFormat.CSV     ⇒ spark.read.csv(path)
    case FileFormat.ORC     ⇒ spark.read.orc(path)
    case FileFormat.Parquet ⇒ spark.read.parquet(path)
    case f                  ⇒ throw new IllegalArgumentException("unknown data source format: " + f)
  }

  protected def load(config: Config)(implicit spark: SparkSession): DataFrame = {
    val sourceConfig = getSourceConfig(config)

    getFormat(sourceConfig) match {
      case FileFormat.CSV     ⇒ loadCsv(sourceConfig)
      case FileFormat.Json    ⇒ loadJson(sourceConfig)
      case FileFormat.ORC     ⇒ loadOrc(sourceConfig)
      case FileFormat.Parquet ⇒ loadParquet(sourceConfig)
    }
  }

  private def loadJson(sourceConfig: Config)(implicit spark: SparkSession): DataFrame = {
    spark.read.json(sourceConfig.getString("path"))
  }

  private def loadCsv(sourceConfig: Config)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .option("delimiter", sourceConfig.getString("delimiter"))
      .option("header", sourceConfig.getBoolean("header").toString)
      .csv(sourceConfig.getString("path"))
  }

  private def loadOrc(sourceConfig: Config)(implicit spark: SparkSession): DataFrame = {
    spark.read.orc(sourceConfig.getString("path"))
  }

  private def loadParquet(sourceConfig: Config)(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(sourceConfig.getString("path"))
  }

  protected def loadCSV(path: String, schema: Seq[String])(implicit spark: SparkSession): DataFrame = {
    schemaFieldsToStructType(schema) match {
      case None      ⇒ throw new IllegalArgumentException("schema is not defined for this source: " + path)
      case Some(sch) ⇒ spark.read.option("delimiter", ";").schema(sch).csv(path)
    }
  }

  private def toDsWithSpecificColumns[T <: Product: TypeTag](df: DataFrame)(implicit spark: SparkSession): Dataset[T] = {
    import spark.implicits._
    val columns = Encoders.product[T].schema.columns
    df.select(columns: _*).as[T]
  }

  private def schemaFieldsToStructType(fieldsNames: Seq[String]): Option[StructType] = {
    if (fieldsNames.isEmpty) None
    else Some(StructType(fields = fieldsNames map { fieldName ⇒ StructField(name = fieldName, dataType = StringType) }))
  }

  private def getSourceConfig(config: Config): Config = config.getConfig(s"sources.$sourceName")
  private def getFormat(sourceConfig: Config): FileFormat = FileFormat(sourceConfig.getString("format"))

}

object Source {

  def apply(name: String): Source = new Source {
    val sourceName: String = name
    val config = ConfigFactory.load()
  }

  /**
    * A config object containing keys 'format' and 'path' can be provided
    */
  def apply(configuration: Config, name: String = ""): Source = new Source {
    val sourceName: String = name
    val config = configuration
    override lazy val sourceFormat: FileFormat = FileFormat(Try(config.getString("format")) getOrElse "parquet")
    override lazy val sourcePath: String = configuration.getString("path")
  }
}