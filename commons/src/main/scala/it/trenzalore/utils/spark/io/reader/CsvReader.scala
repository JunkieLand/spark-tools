package it.trenzalore.utils.spark.io.reader

import it.trenzalore.utils.spark.SparkUtils.implicits._
import it.trenzalore.utils.spark.SparkUtils.schemaOf
import it.trenzalore.utils.spark.io.SourceConfig
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }

import scala.reflect.runtime.universe.TypeTag

object CsvReader extends SourceReader {

  override def loadDf(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .options(sourceConfig.readOptions)
      .option("delimiter", sourceConfig.delimiter.get)
      .option("header", sourceConfig.header.get)
      .csv(sourceConfig.path)
  }

  override def loadDs[T <: Product: TypeTag](sourceConfig: SourceConfig)(implicit spark: SparkSession): Dataset[T] = {
    spark
      .read
      .options(sourceConfig.readOptions)
      .option("delimiter", sourceConfig.delimiter.get)
      .option("header", sourceConfig.header.get)
      .schema(schemaOf[T])
      .csv(sourceConfig.path)
      .to[T]
  }

}
